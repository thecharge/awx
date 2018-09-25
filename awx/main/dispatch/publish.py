import inspect
import logging
import sys
from uuid import uuid4

from django.conf import settings
from kombu import Connection, Exchange, Producer

logger = logging.getLogger('awx.main.dispatch')


def serialize_task(f):
    return '.'.join([f.__module__, f.__name__])


def task(queue=None, exchange_type=None):
    """
    Used to decorate a function or class so that it can be run asynchronously
    via the task dispatcher.  Tasks can be simple functions:

    @task()
    def add(a, b):
        return a + b

    ...or classes that define a `run` method:

    @task
    class Adder:
        def run(self, a, b):
            return a + b

    # Tasks can be run synchronously...
    assert add(1, 1) == 2
    assert Adder().run(1, 1) == 2

    # ...or published to a queue:
    add.apply_async([1, 1])
    Adder.apply_async([1, 1])

    # Tasks can also define a specific target queue or exchange type:

    @task(queue='slow-tasks')
    def snooze():
        time.sleep(10)

    @task(queue='tower_broadcast', exchange_type='fanout')
    def announce():
        print "Run this everywhere!"
    """

    class TaskBase(object):

        queue = None

        @classmethod
        def delay(cls, *args, **kwargs):
            return cls.apply_async(args, kwargs)

        @classmethod
        def apply_async(cls, args=None, kwargs=None, queue=None, uuid=None, **kw):
            task_id = uuid or str(uuid4())
            args = args or []
            kwargs = kwargs or {}
            queue = queue or getattr(cls.queue, 'im_func', cls.queue) or settings.CELERY_DEFAULT_QUEUE
            obj = {
                'uuid': task_id,
                'args': args,
                'kwargs': kwargs,
                'task': cls.name
            }
            obj.update(**kw)
            if callable(queue):
                queue = queue()
            if not settings.IS_TESTING(sys.argv):
                with Connection(settings.BROKER_URL) as conn:
                    exchange = Exchange(queue, type=exchange_type or 'direct')
                    producer = Producer(conn)
                    logger.debug('publish {}({}, queue={})'.format(
                        cls.name,
                        task_id,
                        queue
                    ))
                    producer.publish(obj,
                                     serializer='json',
                                     compression='bzip2',
                                     exchange=exchange,
                                     declare=[exchange],
                                     delivery_mode="persistent",
                                     routing_key=queue)
            return (obj, queue)

    if inspect.isclass(queue):
        # if the wrapped object is a class-based task,
        # add the TaskBase mixin so it has apply_async() and delay()
        ns = dict(queue.__dict__)
        ns['name'] = serialize_task(queue)
        return type(
            queue.__name__,
            tuple(list(queue.__bases__) + [TaskBase]),
            ns
        )

    # it's a function-based task, so return the wrapped/decorated
    # function
    class wrapped_func(TaskBase):

        def __init__(self, f=None):
            if inspect.isclass(f) or f is None:
                # Print a helpful error message if the @task decorator is
                # used incorrectly
                if f is None:
                    err = 'use @task(), not @task'
                    source = inspect.getsourcefile(queue)
                    lines, lnum = inspect.findsource(queue)
                    lnum += 1
                else:
                    err = 'use @task, not @task()'
                    source = inspect.getsourcefile(f)
                    lines, lnum = inspect.findsource(f)
                msg = [
                    source,
                    '{} {}    # <--- {}'.format(
                        lnum - 1, lines[lnum - 1].strip(), err
                    ),
                    '{} {}'.format(lnum, lines[lnum].strip())
                ]
                raise RuntimeError('\n'.join(msg))
            setattr(f, 'apply_async', self.apply_async)
            setattr(f, 'delay', self.delay)
            self.__class__.name = serialize_task(f)
            self.__class__.queue = queue
            self.wrapped = f

        def __call__(self, *args, **kwargs):
            return self.wrapped(*args, **kwargs)

    return wrapped_func
