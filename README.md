Bluebird
--------

Bluebird is a Python client for the [Kestrel](http://robey.github.com/kestrel/ "Kestrel Distributed Queue") distributed queue.


Usage
-----


    from bluebird import Client

    kestrel = Client(host='localhost')
    queue = kestrel['queue-name']

    msg = {'something': 'clever here'}

    queue.put(msg)
    assert queue.get().data == msg


