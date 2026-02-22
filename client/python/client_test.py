
import client.python.subspace as subspace
import server.python.subspace_server as subspace_server
import os
import struct
import tempfile
import unittest


class TestSubspaceClient(unittest.TestCase):
    """Comprehensive tests for the Subspace Python client.

    A single in-process server is started once for the entire test suite
    and stopped when all tests have finished.  Each test uses unique
    channel names to avoid inter-test interference.
    """

    @classmethod
    def setUpClass(cls):
        fd, cls.socket_name = tempfile.mkstemp(dir="/tmp", prefix="subspace")
        os.close(fd)
        cls.server = subspace_server.Server(cls.socket_name, local=True)
        cls.server.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        if os.path.exists(cls.socket_name):
            os.remove(cls.socket_name)

    def _make_client(self, name=""):
        client = subspace.Client()
        client.init(server_socket=self.socket_name, client_name=name)
        return client

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------
    def test_server_is_running(self):
        self.assertTrue(self.server.is_running)
        self.assertEqual(self.server.socket_name, self.socket_name)

    # ------------------------------------------------------------------
    # Basic publish / subscribe (bytes API)
    # ------------------------------------------------------------------
    def test_publish_single_message_and_read(self):
        client = self._make_client("pub_read")
        pub = client.create_publisher(channel_name="ch_single", slot_size=256,
                                      num_slots=10, type="test")
        sub = client.create_subscriber(channel_name="ch_single", type="test")

        pub.publish_message(b"hello")
        sub.wait()
        data = sub.read_message()
        self.assertEqual(data, b"hello")

        # No more messages.
        data = sub.read_message()
        self.assertEqual(len(data), 0)

        pub = None
        sub = None

    def test_publish_multiple_messages(self):
        client = self._make_client("multi")
        pub = client.create_publisher(channel_name="ch_multi", slot_size=256,
                                      num_slots=10)
        sub = client.create_subscriber(channel_name="ch_multi")

        for i in range(5):
            pub.publish_message(f"msg{i}".encode())

        sub.wait()
        messages = []
        while True:
            data = sub.read_message()
            if len(data) == 0:
                break
            messages.append(data)

        self.assertEqual(len(messages), 5)
        for i, m in enumerate(messages):
            self.assertEqual(m, f"msg{i}".encode())

        pub = None
        sub = None

    def test_skip_to_newest(self):
        client = self._make_client("skip")
        pub = client.create_publisher(channel_name="ch_skip", slot_size=256,
                                      num_slots=16)
        sub = client.create_subscriber(channel_name="ch_skip")

        pub.publish_message(b"old")
        pub.publish_message(b"newest")
        sub.wait()

        data = sub.read_message(skip_to_newest=True)
        self.assertEqual(data, b"newest")

        data = sub.read_message()
        self.assertEqual(len(data), 0)

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # Publisher / subscriber accessors
    # ------------------------------------------------------------------
    def test_publisher_accessors(self):
        client = self._make_client("pub_acc")
        pub = client.create_publisher(channel_name="ch_pub_acc",
                                      slot_size=512, num_slots=8,
                                      type="my_type")
        self.assertEqual(pub.type(), "my_type")
        self.assertEqual(pub.slot_size(), 512)
        self.assertEqual(pub.num_slots(), 8)
        self.assertFalse(pub.is_reliable())
        self.assertFalse(pub.is_fixed_size())
        self.assertEqual(pub.name(), "ch_pub_acc")
        self.assertIsInstance(pub.get_virtual_memory_usage(), int)
        self.assertGreater(pub.get_virtual_memory_usage(), 0)
        pub = None

    def test_subscriber_accessors(self):
        client = self._make_client("sub_acc")
        pub = client.create_publisher(channel_name="ch_sub_acc",
                                      slot_size=256, num_slots=10,
                                      type="sub_type")
        sub = client.create_subscriber(channel_name="ch_sub_acc",
                                       type="sub_type")

        pub.publish_message(b"probe")
        sub.wait()
        sub.read_message()

        self.assertEqual(sub.type(), "sub_type")
        self.assertFalse(sub.is_reliable())
        self.assertEqual(sub.slot_size(), 256)
        self.assertEqual(sub.num_slots(), 10)
        self.assertEqual(sub.name(), "ch_sub_acc")
        self.assertIsInstance(sub.get_virtual_memory_usage(), int)
        self.assertGreater(sub.get_virtual_memory_usage(), 0)

        pub = None
        sub = None

    def test_publisher_subscriber_type_match(self):
        client = self._make_client("type_match")
        pub = client.create_publisher(channel_name="ch_types",
                                      slot_size=256, num_slots=10,
                                      type="matching")
        sub = client.create_subscriber(channel_name="ch_types",
                                       type="matching")

        pub.publish_message(b"x")
        sub.wait()
        sub.read_message()

        self.assertEqual(pub.type(), sub.type())
        self.assertEqual(pub.is_reliable(), sub.is_reliable())
        self.assertEqual(pub.slot_size(), sub.slot_size())

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # read_message_object / Message metadata
    # ------------------------------------------------------------------
    def test_read_message_object(self):
        client = self._make_client("msg_obj")
        pub = client.create_publisher(channel_name="ch_msg_obj",
                                      slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_msg_obj")

        pub.publish_message(b"metadata_test")
        sub.wait()

        with sub.read_message_object() as msg:
            self.assertEqual(msg.length, len(b"metadata_test"))
            self.assertEqual(msg.buffer, b"metadata_test")
            self.assertGreater(msg.ordinal, 0)
            self.assertGreater(msg.timestamp, 0)
            self.assertFalse(msg.is_activation)
            self.assertFalse(msg.checksum_error)

        # After exiting the context manager the slot is released;
        # reading again should yield nothing.
        data = sub.read_message()
        self.assertEqual(len(data), 0)

        pub = None
        sub = None

    def test_read_message_object_skip_newest(self):
        client = self._make_client("msg_obj_skip")
        pub = client.create_publisher(channel_name="ch_msg_obj_skip",
                                      slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_msg_obj_skip")

        pub.publish_message(b"first")
        pub.publish_message(b"second")
        sub.wait()

        with sub.read_message_object(skip_to_newest=True) as msg:
            self.assertEqual(msg.buffer, b"second")

        pub = None
        sub = None

    def test_message_ordinals_increase(self):
        client = self._make_client("ordinals")
        pub = client.create_publisher(channel_name="ch_ordinals",
                                      slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_ordinals")

        pub.publish_message(b"a")
        pub.publish_message(b"b")
        sub.wait()

        with sub.read_message_object() as m1:
            ord1 = m1.ordinal
            ts1 = m1.timestamp

        with sub.read_message_object() as m2:
            ord2 = m2.ordinal
            ts2 = m2.timestamp

        self.assertGreater(ord2, ord1)
        self.assertGreaterEqual(ts2, ts1)

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # Zero-copy publishing
    # ------------------------------------------------------------------
    def test_zero_copy_publish(self):
        client = self._make_client("zero_copy")
        pub = client.create_publisher(channel_name="ch_zc",
                                      slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_zc")

        buf = pub.get_message_buffer(256)
        payload = b"zero_copy_data"
        buf[:len(payload)] = payload
        pub_msg = pub.publish_buffer(len(payload))

        self.assertGreater(pub_msg.ordinal, 0)
        self.assertGreater(pub_msg.timestamp, 0)

        sub.wait()
        data = sub.read_message()
        self.assertEqual(data, payload)

        pub = None
        sub = None

    def test_cancel_publish(self):
        client = self._make_client("cancel")
        pub = client.create_publisher(channel_name="ch_cancel",
                                      slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_cancel")

        buf = pub.get_message_buffer(256)
        buf[:4] = b"nope"
        pub.cancel_publish()

        # Nothing was published so reading should return empty.
        data = sub.read_message()
        self.assertEqual(len(data), 0)

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # PublisherOptions / SubscriberOptions
    # ------------------------------------------------------------------
    def test_publisher_options(self):
        opts = subspace.PublisherOptions()
        opts.set_slot_size(1024)
        opts.set_num_slots(4)
        opts.set_reliable(True)
        opts.set_type("opts_type")
        opts.set_local(True)
        opts.set_fixed_size(True)
        opts.set_checksum(True)

        self.assertEqual(opts.slot_size(), 1024)
        self.assertEqual(opts.num_slots(), 4)
        self.assertTrue(opts.is_reliable())
        self.assertEqual(opts.type(), "opts_type")
        self.assertTrue(opts.is_local())
        self.assertTrue(opts.is_fixed_size())
        self.assertTrue(opts.checksum())

    def test_subscriber_options(self):
        opts = subspace.SubscriberOptions()
        opts.set_reliable(True)
        opts.set_type("sub_opts_type")
        opts.set_max_active_messages(5)
        opts.set_checksum(True)
        opts.set_pass_checksum_errors(True)
        opts.set_keep_active_message(True)

        self.assertTrue(opts.is_reliable())
        self.assertEqual(opts.type(), "sub_opts_type")
        self.assertEqual(opts.max_active_messages(), 5)
        self.assertTrue(opts.checksum())
        self.assertTrue(opts.pass_checksum_errors())
        self.assertTrue(opts.keep_active_message())

    def test_create_publisher_with_options(self):
        client = self._make_client("opts_pub")
        opts = subspace.PublisherOptions()
        opts.set_slot_size(128)
        opts.set_num_slots(6)
        opts.set_type("opt_chan_type")

        pub = client.create_publisher(channel_name="ch_opts_pub",
                                      options=opts)
        self.assertEqual(pub.slot_size(), 128)
        self.assertEqual(pub.num_slots(), 6)
        self.assertEqual(pub.type(), "opt_chan_type")
        pub = None

    def test_create_publisher_with_slot_and_options(self):
        client = self._make_client("opts_pub2")
        opts = subspace.PublisherOptions()
        opts.set_type("opt2")
        pub = client.create_publisher(channel_name="ch_opts_pub2",
                                      slot_size=64, num_slots=4,
                                      options=opts)
        self.assertEqual(pub.slot_size(), 64)
        self.assertEqual(pub.num_slots(), 4)
        self.assertEqual(pub.type(), "opt2")
        pub = None

    def test_create_subscriber_with_options(self):
        client = self._make_client("opts_sub")
        client.create_publisher(channel_name="ch_opts_sub",
                                slot_size=128, num_slots=6,
                                type="osub")
        opts = subspace.SubscriberOptions()
        opts.set_type("osub")
        opts.set_max_active_messages(3)

        sub = client.create_subscriber(channel_name="ch_opts_sub",
                                       options=opts)
        self.assertEqual(sub.type(), "osub")
        self.assertFalse(sub.is_reliable())

    # ------------------------------------------------------------------
    # Subscriber before publisher (placeholder)
    # ------------------------------------------------------------------
    def test_subscriber_before_publisher(self):
        client = self._make_client("sub_first")
        sub = client.create_subscriber(channel_name="ch_sub_first")
        pub = client.create_publisher(channel_name="ch_sub_first",
                                      slot_size=256, num_slots=10)

        pub.publish_message(b"late_pub")
        sub.wait()
        data = sub.read_message()
        self.assertEqual(data, b"late_pub")

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # Separate clients for pub and sub
    # ------------------------------------------------------------------
    def test_separate_clients(self):
        pub_client = self._make_client("sep_pub")
        sub_client = self._make_client("sep_sub")

        pub = pub_client.create_publisher(channel_name="ch_sep",
                                          slot_size=256, num_slots=10)
        sub = sub_client.create_subscriber(channel_name="ch_sep")

        pub.publish_message(b"cross_client")
        sub.wait()
        self.assertEqual(sub.read_message(), b"cross_client")

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # Channel info / stats / exists
    # ------------------------------------------------------------------
    def test_channel_exists(self):
        client = self._make_client("exists")
        self.assertFalse(client.channel_exists("ch_no_such_channel"))

        pub = client.create_publisher(channel_name="ch_exists_test",
                                      slot_size=256, num_slots=10)
        self.assertTrue(client.channel_exists("ch_exists_test"))
        pub = None

    def test_channel_info(self):
        client = self._make_client("info")
        pub = client.create_publisher(channel_name="ch_info",
                                      slot_size=512, num_slots=8,
                                      type="info_type")
        sub = client.create_subscriber(channel_name="ch_info",
                                       type="info_type")

        info = client.get_channel_info("ch_info")
        self.assertEqual(info.channel_name, "ch_info")
        self.assertEqual(info.num_publishers, 1)
        self.assertEqual(info.num_subscribers, 1)
        self.assertEqual(info.type, "info_type")
        self.assertEqual(info.slot_size, 512)
        self.assertEqual(info.num_slots, 8)
        self.assertFalse(info.reliable)

        pub = None
        sub = None

    def test_get_all_channel_info(self):
        client = self._make_client("all_info")
        pub = client.create_publisher(channel_name="ch_all_info",
                                      slot_size=256, num_slots=10)

        all_info = client.get_all_channel_info()
        self.assertIsInstance(all_info, list)
        names = [ci.channel_name for ci in all_info]
        self.assertIn("ch_all_info", names)

        pub = None

    def test_channel_stats(self):
        client = self._make_client("stats")
        pub = client.create_publisher(channel_name="ch_stats",
                                      slot_size=256, num_slots=10)
        pub.publish_message(b"stat_msg")

        stats = client.get_channel_stats("ch_stats")
        self.assertEqual(stats.channel_name, "ch_stats")
        self.assertGreater(stats.total_bytes, 0)
        self.assertEqual(stats.total_messages, 1)

        pub = None

    def test_get_all_channel_stats(self):
        client = self._make_client("all_stats")
        pub = client.create_publisher(channel_name="ch_all_stats",
                                      slot_size=256, num_slots=10)
        pub.publish_message(b"x")

        all_stats = client.get_all_channel_stats()
        self.assertIsInstance(all_stats, list)
        names = [cs.channel_name for cs in all_stats]
        self.assertIn("ch_all_stats", names)

        pub = None

    # ------------------------------------------------------------------
    # Channel counters
    # ------------------------------------------------------------------
    def test_channel_counters_from_client(self):
        client = self._make_client("counters")
        pub = client.create_publisher(channel_name="ch_counters",
                                      slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_counters")

        counters = client.get_channel_counters("ch_counters")
        self.assertEqual(counters.num_pubs, 1)
        self.assertEqual(counters.num_subs, 1)

        pub = None
        sub = None

    def test_channel_counters_from_publisher(self):
        client = self._make_client("pub_ctr")
        pub = client.create_publisher(channel_name="ch_pub_ctr",
                                      slot_size=256, num_slots=10)
        counters = pub.get_channel_counters()
        self.assertEqual(counters.num_pubs, 1)
        pub = None

    def test_channel_counters_from_subscriber(self):
        client = self._make_client("sub_ctr")
        pub = client.create_publisher(channel_name="ch_sub_ctr",
                                      slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_sub_ctr")

        pub.publish_message(b"probe")
        sub.wait()
        sub.read_message()

        counters = sub.get_channel_counters()
        self.assertEqual(counters.num_pubs, 1)
        self.assertEqual(counters.num_subs, 1)

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # Client properties
    # ------------------------------------------------------------------
    def test_client_name(self):
        client = self._make_client("named_client")
        self.assertEqual(client.get_name(), "named_client")

    def test_set_debug(self):
        client = self._make_client("debug")
        client.set_debug(True)
        client.set_debug(False)

    def test_set_thread_safe(self):
        client = self._make_client("threadsafe")
        client.set_thread_safe(True)
        client.set_thread_safe(False)

    # ------------------------------------------------------------------
    # Resize callback
    # ------------------------------------------------------------------
    def test_resize_callback(self):
        client = self._make_client("resize")
        pub = client.create_publisher(channel_name="ch_resize",
                                      slot_size=256, num_slots=10)

        resize_log = []

        def on_resize(old_size, new_size):
            resize_log.append((old_size, new_size))
            return True

        pub.register_resize_callback(on_resize)

        # Publish a small message first.
        pub.publish_message(b"small")

        # Publish a message larger than the current slot_size to trigger resize.
        pub.publish_message(b"x" * 1024)

        self.assertEqual(len(resize_log), 1)
        self.assertEqual(resize_log[0][0], 256)
        self.assertEqual(resize_log[0][1], 1024)

        pub.unregister_resize_callback()
        pub = None

    # ------------------------------------------------------------------
    # Message callback / process_all_messages
    # ------------------------------------------------------------------
    def test_message_callback_and_process_all(self):
        pub_client = self._make_client("cb_pub")
        sub_client = self._make_client("cb_sub")

        pub = pub_client.create_publisher(channel_name="ch_callback",
                                          slot_size=256, num_slots=10)

        opts = subspace.SubscriberOptions()
        opts.set_max_active_messages(3)
        sub = sub_client.create_subscriber(channel_name="ch_callback",
                                           options=opts)

        # Store the Message objects themselves so that slots are held.
        received = []

        def on_message(msg):
            received.append(msg)

        sub.register_message_callback(on_message)

        for i in range(6):
            pub.publish_message(f"foobar{i}".encode()[:6])

        sub.wait()
        sub.process_all_messages()

        # With max_active_messages=3, holding each Message keeps the slot
        # occupied, so we should get at most 3 in the first batch.
        self.assertEqual(len(received), 3)

        # Release one message and process again to get one more.
        received.pop().Reset()
        sub.process_all_messages()
        self.assertEqual(len(received), 3)

        # Clean up held messages.
        for m in received:
            m.Reset()

        sub.unregister_message_callback()
        pub = None
        sub = None

    # ------------------------------------------------------------------
    # get_all_messages
    # ------------------------------------------------------------------
    def test_get_all_messages(self):
        client = self._make_client("get_all")
        pub = client.create_publisher(channel_name="ch_get_all",
                                      slot_size=256, num_slots=10)
        opts = subspace.SubscriberOptions()
        opts.set_max_active_messages(5)
        sub = client.create_subscriber(channel_name="ch_get_all",
                                       options=opts)

        pub.publish_message(b"one")
        pub.publish_message(b"two")
        pub.publish_message(b"three")
        sub.wait()

        messages = sub.get_all_messages()
        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0].buffer, b"one")
        self.assertEqual(messages[1].buffer, b"two")
        self.assertEqual(messages[2].buffer, b"three")

        for m in messages:
            m.Reset()

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # Dropped message callback
    # ------------------------------------------------------------------
    def test_dropped_message_callback(self):
        pub_client = self._make_client("drop_pub")
        sub_client = self._make_client("drop_sub")

        pub = pub_client.create_publisher(channel_name="ch_drop",
                                          slot_size=256, num_slots=10)

        opts = subspace.SubscriberOptions()
        opts.set_max_active_messages(3)
        sub = sub_client.create_subscriber(channel_name="ch_drop",
                                           options=opts)

        dropped_counts = []

        def on_drop(n):
            dropped_counts.append(n)

        sub.register_dropped_message_callback(on_drop)

        # Prime the subscriber by reading one message so it knows what
        # ordinal to expect next.
        pub.publish_message(b"prime!")
        sub.wait()
        data = sub.read_message()
        self.assertEqual(data, b"prime!")

        # Flood the channel with more messages than slots can hold.
        for i in range(20):
            pub.publish_message(f"flood{i}".encode()[:6])

        # Drain all available messages.
        while True:
            data = sub.read_message()
            if len(data) == 0:
                break

        # We should have seen some drops. With 10 slots, 1 pub, 1 sub
        # there are ~7 usable slots, so 20 messages => ~11 dropped.
        total_dropped = sum(dropped_counts)
        self.assertGreater(total_dropped, 0)

        sub.unregister_dropped_message_callback()
        pub = None
        sub = None

    # ------------------------------------------------------------------
    # Duplicate publisher on same channel should fail
    # ------------------------------------------------------------------
    def test_duplicate_publisher_fails(self):
        client = self._make_client("dup_pub")
        pub_opts = subspace.PublisherOptions()
        pub_opts.set_slot_size(256)
        pub_opts.set_num_slots(10)
        pub_opts.set_type("dup_type")
        pub = client.create_publisher(channel_name="ch_dup", options=pub_opts)
        self.assertIsNotNone(pub)

        # A second publisher with a different num_slots should fail.
        bad_opts = subspace.PublisherOptions()
        bad_opts.set_slot_size(256)
        bad_opts.set_num_slots(100)
        with self.assertRaises(RuntimeError):
            client.create_publisher(channel_name="ch_dup", options=bad_opts)

        pub = None

    # ------------------------------------------------------------------
    # Poll file descriptors are valid
    # ------------------------------------------------------------------
    def test_publisher_poll_fd(self):
        client = self._make_client("poll_pub")
        pub = client.create_publisher(channel_name="ch_poll_pub",
                                      slot_size=256, num_slots=10)
        fd = pub.get_poll_fd()
        self.assertIsInstance(fd, int)
        pub = None

    def test_subscriber_poll_fd(self):
        client = self._make_client("poll_sub")
        client.create_publisher(channel_name="ch_poll_sub",
                                slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_poll_sub")
        fd = sub.get_poll_fd()
        self.assertIsInstance(fd, int)
        sub = None

    def test_publisher_file_descriptor(self):
        client = self._make_client("fd_pub")
        pub = client.create_publisher(channel_name="ch_fd_pub",
                                      slot_size=256, num_slots=10)
        fd = pub.get_file_descriptor()
        self.assertIsInstance(fd, int)
        pub = None

    def test_subscriber_file_descriptor(self):
        client = self._make_client("fd_sub")
        client.create_publisher(channel_name="ch_fd_sub",
                                slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_fd_sub")
        fd = sub.get_file_descriptor()
        self.assertIsInstance(fd, int)
        sub = None

    # ------------------------------------------------------------------
    # find_message by timestamp
    # ------------------------------------------------------------------
    def test_find_message(self):
        client = self._make_client("find")
        pub = client.create_publisher(channel_name="ch_find",
                                      slot_size=256, num_slots=10)
        opts = subspace.SubscriberOptions()
        opts.set_max_active_messages(3)
        sub = client.create_subscriber(channel_name="ch_find",
                                       options=opts)

        pub.publish_message(b"findme")
        sub.wait()

        with sub.read_message_object() as msg:
            ts = msg.timestamp

        # find_message should locate the same message by its timestamp.
        with sub.find_message(ts) as found:
            self.assertEqual(found.buffer, b"findme")
            self.assertEqual(found.timestamp, ts)

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # trigger / untrigger
    # ------------------------------------------------------------------
    def test_trigger_untrigger(self):
        client = self._make_client("trigger")
        pub = client.create_publisher(channel_name="ch_trigger",
                                      slot_size=256, num_slots=10)
        sub = client.create_subscriber(channel_name="ch_trigger")

        sub.trigger()
        sub.untrigger()

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # num_active_messages / clear_active_message
    # ------------------------------------------------------------------
    def test_active_messages(self):
        client = self._make_client("active")
        pub = client.create_publisher(channel_name="ch_active",
                                      slot_size=256, num_slots=10)

        opts = subspace.SubscriberOptions()
        opts.set_max_active_messages(3)
        sub = client.create_subscriber(channel_name="ch_active",
                                       options=opts)

        pub.publish_message(b"one")
        sub.wait()

        with sub.read_message_object() as msg:
            self.assertEqual(msg.length, 3)

        sub.clear_active_message()

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # num_subscribers from publisher
    # ------------------------------------------------------------------
    def test_num_subscribers(self):
        client = self._make_client("num_subs")
        pub = client.create_publisher(channel_name="ch_num_subs",
                                      slot_size=256, num_slots=10)
        sub1 = client.create_subscriber(channel_name="ch_num_subs")
        sub2 = client.create_subscriber(channel_name="ch_num_subs")

        self.assertEqual(pub.num_subscribers(), 2)

        sub1 = None
        sub2 = None
        pub = None

    # ------------------------------------------------------------------
    # ReadMode enum
    # ------------------------------------------------------------------
    def test_read_mode_enum(self):
        self.assertIsNotNone(subspace.ReadMode.READ_NEXT)
        self.assertIsNotNone(subspace.ReadMode.READ_NEWEST)
        self.assertNotEqual(subspace.ReadMode.READ_NEXT,
                            subspace.ReadMode.READ_NEWEST)

    # ------------------------------------------------------------------
    # Large message
    # ------------------------------------------------------------------
    def test_large_message(self):
        client = self._make_client("large")
        pub = client.create_publisher(channel_name="ch_large",
                                      slot_size=65536, num_slots=4)
        sub = client.create_subscriber(channel_name="ch_large")

        payload = b"A" * 60000
        pub.publish_message(payload)
        sub.wait()
        data = sub.read_message()
        self.assertEqual(data, payload)

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # Publishing a zero-length message is rejected by the server
    # ------------------------------------------------------------------
    def test_empty_message_rejected(self):
        client = self._make_client("empty")
        pub = client.create_publisher(channel_name="ch_empty",
                                      slot_size=256, num_slots=10)

        with self.assertRaises(RuntimeError):
            pub.publish_message(b"")

        pub = None

    # ------------------------------------------------------------------
    # Reliable publisher / subscriber
    # ------------------------------------------------------------------
    def test_reliable_pub_sub(self):
        client = self._make_client("reliable")
        pub = client.create_publisher(channel_name="ch_reliable",
                                      slot_size=256, num_slots=10,
                                      reliable=True)
        sub = client.create_subscriber(channel_name="ch_reliable",
                                       reliable=True)

        self.assertTrue(pub.is_reliable())
        self.assertTrue(sub.is_reliable())

        pub.publish_message(b"reliable_msg")
        sub.wait()
        data = sub.read_message()
        self.assertEqual(data, b"reliable_msg")

        pub = None
        sub = None

    # ------------------------------------------------------------------
    # Server context manager
    # ------------------------------------------------------------------
    def test_server_context_manager(self):
        fd, sock = tempfile.mkstemp(dir="/tmp", prefix="subspace_cm")
        os.close(fd)
        try:
            with subspace_server.Server(sock, local=True) as srv:
                self.assertTrue(srv.is_running)
                c = subspace.Client()
                c.init(server_socket=sock, client_name="cm_test")
            self.assertFalse(srv.is_running)
        finally:
            if os.path.exists(sock):
                os.remove(sock)


if __name__ == "__main__":
    unittest.main()
