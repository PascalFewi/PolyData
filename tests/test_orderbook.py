import unittest
from datetime import datetime
from src.models import Order_Book, OrderSummary, Orderbook_Track, Updates, Changes
from src.orderbook import orderbook_get_updates

class TestOrderbookGetUpdates(unittest.TestCase):
    def test_orderbook_get_updates(self):
        """
        Test orderbook_get_updates with various scenarios, including additions, modifications, and deletions.
        """
        # Initial order book
        old_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:00:00Z",
            hash="hash1",
            timestamp="2024-12-17T00:00:00Z",
            bids=[
                OrderSummary(price=100.0, size=10.0),
                OrderSummary(price=99.0, size=5.0),
                OrderSummary(price=98.0, size=1.0),
            ],
            asks=[
                OrderSummary(price=101.0, size=8.0),
                OrderSummary(price=102.0, size=4.0),
                OrderSummary(price=103.0, size=2.0),
            ]
        )

        # Updated order book
        new_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:15:00Z",
            hash="hash2",
            timestamp="2024-12-17T00:15:00Z",
            bids=[
                OrderSummary(price=100.0, size=12.0),  # Modified
                OrderSummary(price=98.0, size=1.0),
                OrderSummary(price=97.0, size=3.0),   # Added
            ],
            asks=[
                OrderSummary(price=101.0, size=8.0),  # Unchanged
                OrderSummary(price=102.0, size=6.0),  # Modified
                OrderSummary(price=104.0, size=5.0),  # Added
            ]
        )

        # Call the function
        updates = orderbook_get_updates(old_orderbook, new_orderbook)

        # Expected changes
        expected_changes = Changes(
            bids=[
                OrderSummary(price=100.0, size=12.0), # Modified
                OrderSummary(price=99.0, size=0.0),   # Removed
                OrderSummary(price=97.0, size=3.0),   # Added
            ],
            asks=[
                OrderSummary(price=104.0, size=5.0),  # Added
                OrderSummary(price=103.0, size=0.0),  # removed
                OrderSummary(price=102.0, size=6.0),  # Modified

            ]
        )

        # Assertions for bids
        self.assertEqual(len(updates.changes.bids), len(expected_changes.bids))
        for update, expected in zip(updates.changes.bids, expected_changes.bids):
            self.assertEqual(update.price, expected.price)
            self.assertEqual(update.size, expected.size)

        # Assertions for asks
        self.assertEqual(len(updates.changes.asks), len(expected_changes.asks))
        for update, expected in zip(updates.changes.asks, expected_changes.asks):
            self.assertEqual(update.price, expected.price)
            self.assertEqual(update.size, expected.size)

        # Assert timestamp is updated correctly
        self.assertEqual(updates.timestamp, "2024-12-17T00:15:00Z")
    def test_orderbook_get_updates_extended(self):
        """
        Test orderbook_get_updates with multiple scenarios, including:
        - Initial order book
        - Two updates with changes
        - One update without changes
        - Keeping track of updates in an Orderbook_Track object
        """
        # Initial order book
        initial_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:00:00Z",
            hash="hash1",
            timestamp="2024-12-17T00:00:00Z",
            bids=[
                OrderSummary(price=100.0, size=10.0),
                OrderSummary(price=99.0, size=5.0),
                OrderSummary(price=98.0, size=1.0),
            ],
            asks=[
                OrderSummary(price=101.0, size=8.0),
                OrderSummary(price=102.0, size=4.0),
                OrderSummary(price=103.0, size=2.0),
            ]
        )

        # First update with changes
        first_update_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:15:00Z",
            hash="hash2",
            timestamp="2024-12-17T00:15:00Z",
            bids=[
                OrderSummary(price=100.0, size=12.0),  # Modified
                OrderSummary(price=98.0, size=1.0),   # Unchanged
                OrderSummary(price=97.0, size=3.0),   # Added
            ],
            asks=[
                OrderSummary(price=101.0, size=8.0),  # Unchanged
                OrderSummary(price=102.0, size=6.0),  # Modified
                OrderSummary(price=104.0, size=5.0),  # Added
            ]
        )

        # Second update with changes
        second_update_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:30:00Z",
            hash="hash3",
            timestamp="2024-12-17T00:30:00Z",
            bids=[
                OrderSummary(price=100.0, size=12.0),  # Unchanged
                OrderSummary(price=97.0, size=0.0),   # Removed
            ],
            asks=[
                OrderSummary(price=101.0, size=8.0),  # Unchanged
                OrderSummary(price=103.0, size=2.0),  # Added back
            ]
        )

        # Third update without changes
        third_update_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:45:00Z",
            hash="hash3",  # Same hash, indicating no changes
            timestamp="2024-12-17T00:45:00Z",
            bids=[
                OrderSummary(price=100.0, size=12.0),  # Unchanged
            ],
            asks=[
                OrderSummary(price=101.0, size=8.0),  # Unchanged
                OrderSummary(price=103.0, size=2.0),  # Unchanged
            ]
        )

        # Initialize the Orderbook_Track object
        orderbook_track = Orderbook_Track(
            id="market-1",
            slug="will-bitcoin-hit",
            fetched_at="2024-12-17T00:00:00Z",
            hour=0,
            start_orderbook=initial_orderbook,
            start_time_stamp=initial_orderbook.timestamp,
            condition_id="condition-123",
            order_price_min_tick_size=0.01,
            order_min_size=1.0,
            clob_token_id="asset-1",
            updates=[]
        )

        # Apply updates and track them
        updates = []
        updates.append(orderbook_get_updates(initial_orderbook, first_update_orderbook))
        updates.append(orderbook_get_updates(first_update_orderbook, second_update_orderbook))
        updates.append(orderbook_get_updates(second_update_orderbook, third_update_orderbook))

        # Add updates to the Orderbook_Track
        orderbook_track.updates.extend(updates)

        # Assertions for updates
        self.assertEqual(len(orderbook_track.updates), 3)

        # Check first update
        self.assertEqual(len(orderbook_track.updates[0].changes.bids), 3)  # 1 modified, 1 unchanged, 1 added
        self.assertEqual(len(orderbook_track.updates[0].changes.asks), 3)  # 1 unchanged, 1 modified, 1 added

        # Check second update
        self.assertEqual(len(orderbook_track.updates[1].changes.bids), 1)  # 1 removed
        self.assertEqual(len(orderbook_track.updates[1].changes.asks), 1)  # 1 added back

        # Check third update
        self.assertEqual(len(orderbook_track.updates[2].changes.bids), 0)  # No changes
        self.assertEqual(len(orderbook_track.updates[2].changes.asks), 0)  # No changes

        # Validate timestamps
        self.assertEqual(orderbook_track.updates[0].timestamp, "2024-12-17T00:15:00Z")
        self.assertEqual(orderbook_track.updates[1].timestamp, "2024-12-17T00:30:00Z")
        self.assertEqual(orderbook_track.updates[2].timestamp, "2024-12-17T00:45:00Z")

    def test_no_changes(self):
        """
        Test when no changes occur between the order books.
        """
        old_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:00:00Z",
            hash="hash1",
            timestamp="2024-12-17T00:00:00Z",
            bids=[
                OrderSummary(price=100.0, size=10.0),
                OrderSummary(price=99.0, size=5.0),
            ],
            asks=[
                OrderSummary(price=101.0, size=8.0),
                OrderSummary(price=102.0, size=4.0),
            ]
        )

        new_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:15:00Z",
            hash="hash2",
            timestamp="2024-12-17T00:15:00Z",
            bids=[
                OrderSummary(price=100.0, size=10.0),
                OrderSummary(price=99.0, size=5.0),
            ],
            asks=[
                OrderSummary(price=101.0, size=8.0),
                OrderSummary(price=102.0, size=4.0),
            ]
        )

        # Call the function
        updates = orderbook_get_updates(old_orderbook, new_orderbook)

        # Assertions
        self.assertEqual(len(updates.changes.bids), 0)
        self.assertEqual(len(updates.changes.asks), 0)
        self.assertEqual(updates.timestamp, "2024-12-17T00:15:00Z")

    def test_all_removed(self):
        """
        Test when all bids and asks are removed.
        """
        old_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:00:00Z",
            hash="hash1",
            timestamp="2024-12-17T00:00:00Z",
            bids=[
                OrderSummary(price=100.0, size=10.0),
                OrderSummary(price=99.0, size=5.0),
            ],
            asks=[
                OrderSummary(price=101.0, size=8.0),
                OrderSummary(price=102.0, size=4.0),
            ]
        )

        new_orderbook = Order_Book(
            market="market-1",
            asset_id="asset-1",
            fetched_at="2024-12-17T00:15:00Z",
            hash="hash2",
            timestamp="2024-12-17T00:15:00Z",
            bids=[],
            asks=[]
        )

        # Call the function
        updates = orderbook_get_updates(old_orderbook, new_orderbook)

        # Assertions
        self.assertEqual(len(updates.changes.bids), 2)  # Both removed
        self.assertEqual(len(updates.changes.asks), 2)  # Both removed

        for bid in updates.changes.bids:
            self.assertEqual(bid.size, 0)

        for ask in updates.changes.asks:
            self.assertEqual(ask.size, 0)

        self.assertEqual(updates.timestamp, "2024-12-17T00:15:00Z")


if __name__ == "__main__":
    unittest.main()
