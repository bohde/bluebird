from mock import Mock
from nose.tools import ok_

from bluebird.client import Client


def test_it_delegates_serialize_to_serializer():
    serializer = Mock()
    c = Client(serializer=serializer)

    message = {'some': 'message'}

    c.serialize(message)
    serializer.serialize.assert_called_once_with(message)


def test_it_delegates_deserialize_to_serializer():
    serializer = Mock()
    c = Client(serializer=serializer)

    data = "some string"
    result = Mock(data=data)

    c.deserialize(result)
    serializer.deserialize.assert_called_once_with(data)
