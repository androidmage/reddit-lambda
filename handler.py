from pymongo import MongoClient
import time

connection_uri = "mongodb+srv://allenadmin:adminpass@shopping-list-tutorial-5jacx.mongodb.net/mern_shopping?retryWrites=true&w=majority"
client = MongoClient(connection_uri)
db = client["mern_shopping"]
in_col = db["reddit_comments"]
hour_col = db["hour_agg"]
sentiment_col = db["sentiment_agg"]


def aggregate(event, context):
    day = 86400
    end = time.time() - day
    start = end - day
    hour_query = [{
    "$match" : 
        {"created_utc" : {"$gte" : start, "$lte" : end}}
    },
    {
    "$group" : 
        {"_id" : "$hour",
         "count" : {"$sum" : 1}
         }}
    ]

    sentiment_query = [{
    "$match" : 
        {"created_utc" : {"$gte" : start, "$lte" : end}}
    },
    {
    "$group" : 
        {"_id" : "$sentiment_score",
         "count" : {"$sum" : 1}
         }}
    ]

    hour_result = list(in_col.aggregate(hour_query))
    sentiment_result = list(in_col.aggregate(sentiment_query))

    # print(hour_result)
    # print(sentiment_result)

    # delete old database entries

    hour_col.delete_many({})
    hour_col.insert_many(hour_result)
    sentiment_col.delete_many({})
    sentiment_col.insert_many(sentiment_result)

    return "done"