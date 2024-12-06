# Databricks notebook source
# MAGIC %run "./remove_uri_from_player"

# COMMAND ----------

import unittest

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_removesUriSingleRow(self):
        players = [Row(id="1", name="Player with a URI", uri="test", rel="user")]
        expected = [Row(id="1", name="Player with a URI", rel="user")]
        
        self.assertEqual(removeUriFromPlayers(players)[0].asDict(), expected[0].asDict())

    def test_removesUriMultipleRows(self):
        players = [Row(id="1", name="Player with a URI", uri="test", rel="user"), Row(id="2", name="2nd Player with a URI", uri="test2", rel="user")]
        expected = [Row(id="1", name="Player with a URI", rel="user"), Row(id="2", name="2nd Player with a URI", rel="user")]
        
        for i in range(len(expected)):
            self.assertEqual(removeUriFromPlayers(players)[i].asDict(), expected[i].asDict())

    def test_handleNoId(self):
        players = [Row(name="Player with no ID", rel="user", uri="test")]
        expected = [Row(id=None, name="Player with no ID", rel="user")]

        self.assertEqual(removeUriFromPlayers(players)[0].asDict(), expected[0].asDict())

    def test_handlesNoName(self):
        players = [Row(id="1", rel="user", uri="test")]
        expected = [Row(id="1", name=None, rel="user")]

        self.assertEqual(removeUriFromPlayers(players)[0].asDict(), expected[0].asDict())

    def test_throwValueErrorNoRel(self):
        players = [Row(id="1", name="Player with no rel", uri="test")]
        with self.assertRaises(Exception):
            removeUriFromPlayers(players)

    def test_throwValueErrorNoUri(self):
        players = [Row(id="1", name="Player with no URI", rel="user")]
        with self.assertRaises(Exception):
            removeUriFromPlayers(players)

    def test_throwValueErrorMixed(self):
        players = [Row(id="1", name="Player with no URI", rel="user"), Row(id="2", name="Player with a URI", uri="test", rel="user")]
        with self.assertRaises(Exception):
            removeUriFromPlayers(players)

# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
