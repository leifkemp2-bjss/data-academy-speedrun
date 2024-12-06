# Databricks notebook source
from datetime import date

# COMMAND ----------

test_runs_valid = [ { "id": "yoqv235z", "weblink": "https://www.speedrun.com/little_nightmares/run/yoqv235z", "game": "j1lewj6g", "level": "kwjekv19", "category": "xd131x4k", "comment": "sub 10 is doable", "status": { "status": "verified", "examiner": "j2wqv36j", "verify-date": "2024-11-28T20:39:07Z" }, "players": [ { "rel": "user", "id": "xk1kw9kj", "uri": "https://www.speedrun.com/api/v1/users/xk1kw9kj" } ], "date": "2024-11-27", "submitted": "2024-11-27T02:53:54Z", "times": { "primary": "PT10M1.110S", "primary_t": 601.11 }, "system": { "platform": "8gej2n93", "emulated": False, "region": None }, "splits": None, "values": {} } ]

# COMMAND ----------

test_runs_valid_expected = [ { "id": "yoqv235z", "game": "j1lewj6g", "level": "kwjekv19", "category": "xd131x4k", "date": "2024-11-27", "submitted": "2024-11-27T02:53:54Z", "players": [ { "rel": "user", "name": None, "id": "xk1kw9kj" } ], "reviewer": "j2wqv36j",  "time": 601.11, "submitted_date": date(2024, 11, 27), "status": "verified", "status_comment": None } ]

# COMMAND ----------

test_runs_noId = [ { "weblink": "https://www.speedrun.com/little_nightmares/run/yoqv235z", "game": "j1lewj6g", "level": "kwjekv19", "category": "xd131x4k", "comment": "sub 10 is doable", "status": { "status": "verified", "examiner": "j2wqv36j", "verify-date": "2024-11-28T20:39:07Z" }, "players": [ { "rel": "user", "id": "xk1kw9kj", "uri": "https://www.speedrun.com/api/v1/users/xk1kw9kj" } ], "date": "2024-11-27", "submitted": "2024-11-27T02:53:54Z", "times": { "primary": "PT10M1.110S", "primary_t": 601.11 }, "system": { "platform": "8gej2n93", "emulated": False, "region": None }, "splits": None, "values": {} } ]
