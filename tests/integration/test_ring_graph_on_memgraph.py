"""The payment graph and its ring features against a real Memgraph in a throwaway container.

Needs Docker; skipped without it. Run with `./cicd/test.sh integration`.
"""

from __future__ import annotations

import time
from unittest import SkipTest, TestCase, main

from fraudstream.graph.ring import RingFeatures, add_payments, ring_features

MEMGRAPH_IMAGE = "memgraph/memgraph:3.14.0"
START, END = 1_000_000, 2_000_000


def payment(transaction_id, customer_id, device_id="dev_ring_0001", ip_address=None, at=START, fraud=False):
    return {"transaction_id": transaction_id, "customer_id": customer_id, "device_id": device_id,
            "ip_address": ip_address, "at": at, "fraud": fraud}


class RingGraphOnMemgraphTest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        try:
            from testcontainers.core.container import DockerContainer
            import docker
            from neo4j import GraphDatabase

            docker.from_env().ping()
        except Exception as error:  # no package, or no Docker daemon
            raise SkipTest(f"Docker isn't available: {error}") from None

        cls.memgraph = DockerContainer(MEMGRAPH_IMAGE).with_exposed_ports(7687)
        cls.memgraph.start()
        cls.addClassCleanup(cls.memgraph.stop)
        uri = f"bolt://{cls.memgraph.get_container_host_ip()}:{cls.memgraph.get_exposed_port(7687)}"
        cls.driver = GraphDatabase.driver(uri, auth=("", ""))
        cls.addClassCleanup(cls.driver.close)
        _wait_until_ready(cls.driver)

    def setUp(self) -> None:
        self.session = self.driver.session()
        self.addCleanup(self.session.close)
        self.session.run("MATCH (n) DETACH DELETE n").consume()

    def features(self, device_id: str) -> RingFeatures:
        (row,) = ring_features(self.session, [device_id], START, END)
        return row

    def test_three_customers_on_one_device_are_counted_once_each(self) -> None:
        add_payments(self.session, [payment("t1", "c1"), payment("t2", "c2"),
                                    payment("t3", "c2", fraud=True), payment("t4", "c3")])

        self.assertEqual(self.features("dev_ring_0001"), RingFeatures("dev_ring_0001", 3, 1, 0))

    def test_customers_sharing_an_ip_are_two_hops_away(self) -> None:
        add_payments(self.session, [payment("t1", "c1", "dev_a", "10.0.0.1"),
                                    payment("t2", "c2", "dev_b", "10.0.0.1")])

        features = self.features("dev_a")

        self.assertEqual((features.customer_count, features.ip_customer_count), (1, 2))

    def test_payments_outside_the_window_are_not_counted(self) -> None:
        add_payments(self.session, [payment("t1", "c1", at=START), payment("t2", "c2", at=END)])

        self.assertEqual(self.features("dev_ring_0001").customer_count, 1)

    def test_adding_the_same_payment_twice_changes_nothing(self) -> None:
        rows = [payment("t1", "c1", ip_address="10.0.0.1", fraud=True), payment("t2", "c2")]
        add_payments(self.session, rows)
        once = self.features("dev_ring_0001")

        add_payments(self.session, rows)

        self.assertEqual(self.features("dev_ring_0001"), once)
        self.assertEqual(self.session.run("MATCH (p:Payment) RETURN count(p) AS n").single()["n"], 2)

    def test_a_payment_without_device_or_ip_adds_only_its_customer(self) -> None:
        add_payments(self.session, [payment("t1", "c1", device_id=None, ip_address=None)])

        edges = self.session.run("MATCH (p:Payment)-[r]->() RETURN count(r) AS n").single()["n"]

        self.assertEqual(edges, 0)

    def test_an_unknown_device_still_gets_a_row_of_zeros(self) -> None:
        self.assertEqual(self.features("dev_nobody"), RingFeatures("dev_nobody", 0, 0, 0))


def _wait_until_ready(driver, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            driver.verify_connectivity()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


if __name__ == "__main__":
    main()
