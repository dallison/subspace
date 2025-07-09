
from python.runfiles import runfiles

import client.python.subspace as subspace
import os
import subprocess
import tempfile
import time
import unittest

class TestSubspaceClient(unittest.TestCase):
    def setUp(self):
        (socket_fd, self.socket_name) = tempfile.mkstemp(dir="/tmp", prefix="subspace")
        socket_file = os.fdopen(socket_fd, "w")
        socket_file.close()

        r = runfiles.Create()

        self.server_proc = subprocess.Popen([r.Rlocation("_main/server/subspace_server"), "--local", "--socket=" + self.socket_name])

        # Wait for client to be able to init.
        waiting_client = subspace.Client()
        while True:
            try:
                waiting_client.init(server_socket=self.socket_name, client_name="waiting_client")
                break
            except:
                time.sleep(0.0001)
        waiting_client = None

    def tearDown(self):
        self.server_proc.terminate()
        self.server_proc.wait()
        os.remove(self.socket_name)

    def test_one_message(self):
        client = subspace.Client()
        client.init(server_socket=self.socket_name, client_name="one_message_client")

        pub = client.create_publisher(
            channel_name="dave0", slot_size=256, num_slots=16, type="foobar")

        sub = client.create_subscriber(
            channel_name="dave0", type="foobar")

        pub.publish_message(b'Hello world!')

        sub.wait()

        # Check we get the right message:
        received_message = sub.read_message().decode('utf-8')
        self.assertEqual(received_message, "Hello world!")

        # Note, accessors of subscriber can only be used after first read_message.
        self.assertEqual(sub.type(), pub.type())
        self.assertEqual(sub.is_reliable(), pub.is_reliable())
        self.assertEqual(sub.slot_size(), pub.slot_size())
        
        # Check that no other message exists:
        received_message = sub.read_message().decode('utf-8')
        self.assertEqual(len(received_message), 0)

        pub.publish_message(b'Hello again!')

        sub.wait()

        # New message appeared:
        received_message = sub.read_message().decode('utf-8')
        self.assertEqual(received_message, "Hello again!")

        # Check that no other message exists:
        received_message = sub.read_message().decode('utf-8')
        self.assertEqual(len(received_message), 0)

        # Must destroy sub / pub before client goes away.
        # Why, oh why, doesn't Python have RAII!
        pub = None
        sub = None

    def test_two_message_skip_one(self):
        client = subspace.Client()
        client.init(server_socket=self.socket_name, client_name="one_message_client")

        pub = client.create_publisher(
            channel_name="dave0", slot_size=256, num_slots=16, type="foobar")

        sub = client.create_subscriber(
            channel_name="dave0", type="foobar")

        pub.publish_message(b'Hello world!')
        pub.publish_message(b'Hello again!')

        sub.wait()

        # Check we get the right message:
        received_message = sub.read_message(skip_to_newest=True).decode('utf-8')
        self.assertEqual(received_message, "Hello again!")

        # Check that no other message exists:
        received_message = sub.read_message().decode('utf-8')
        self.assertEqual(len(received_message), 0)

        # Must destroy sub / pub before client goes away.
        # Why, oh why, doesn't Python have RAII!
        pub = None
        sub = None


if __name__ == '__main__':
    unittest.main()

