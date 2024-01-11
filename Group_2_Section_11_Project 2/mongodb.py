import pymongo
import time
client = pymongo.MongoClient("mongodb://localhost:27017/") 

project = client["project"] 
ls = project["ls"]   

# total number of stores in the city of Waterloo
# tt = 1.53009605408
start_time = time.time()
count = ls.count_documents({"city": "Waterloo"})
end_time = time.time()
time_taken = end_time - start_time
print('Time taken: ', time_taken)
print(count)


# invoices with amount between 500 and 50000
# tt = 0.000107049942017
start_time = time.time()
results = ls.find({"sale_usd": {"$gte": 500, "$lte": 5000}}, {"invoice_number": 1, "date": 1, "store_name": 1, "sale_usd": 1})
end_time = time.time()
time_taken = end_time - start_time
print('Time taken: ', time_taken)
for result in results:
    print(result)

#  which years has made the most profit and the number of bottles sold they sold
# tt = 2.909611940383911
start_time = time.time()
pipeline = [
    {
        "$group": {
            "_id": {"$year": "$date"},
            "total_bottles_sold": {"$sum": "$bottles_sold"},
            "total_sale_usd": {"$sum": "$sale_usd"}
        }
    },
    {
        "$sort": {"total_sale_usd": -1}
    }
]
result = ls.aggregate(pipeline)
end_time = time.time()
time_taken = end_time - start_time
print('Time taken: ', time_taken)
for document in result:
    print(document)


# total number of packs each store has sold
# tt = 1.8097178936
start_time = time.time()
pipeline = [
    {"$group": {"_id": "$store_number", "pack": {"$sum": "$pack"}}},
    {"$sort": {"pack": -1}}
]
results = ls.aggregate(pipeline)
end_time = time.time()
time_taken = end_time - start_time
print('Time taken: ', time_taken)
for result in results:
    print(result)


# vendors at different shops who have sold more than 5000 bottles and more than 50000 dollars
# tt = 3.4996519088745117
start_time = time.time()
pipeline = [
    {
        "$group": {
            "_id": {
                "store_number": "$store_number",
                "vendor_number": "$vendor_number"
            },
            "bottles_sold": {"$sum": "$bottles_sold"},
            "sales_usd": {"$sum": "$sale_usd"}
        }
    },
    {
        "$match": {
            "bottles_sold": {"$gt": 1000},
            "sales_usd": {"$gt": 500000}
        }
    },
    {
        "$sort": {"_id.store_number": 1, "_id.vendor_number": -1}
    }
]
result = ls.aggregate(pipeline)
end_time = time.time()
time_taken = end_time - start_time
print('Time taken: ', time_taken)
for document in result:
    print(document)


# stores that sold bottles more than the average
# tt = 1.94035696983
start_time = time.time()
avg_bottles_sold = ls.aggregate([
    {"$group": {"_id": None, "avg_bottles_sold": {"$avg": "$bottles_sold"}}}
]).next()["avg_bottles_sold"]
result = ls.aggregate([
    {
        "$match": {
            "bottles_sold": {"$gt": avg_bottles_sold}
        }
    },
    {
        "$project": {
            "store_name": 1,
            "address": 1,
            "city": 1,
            "county": 1,
            "bottles_sold": 1
        }
    }
])
end_time = time.time()
time_taken = end_time - start_time
print('Time taken: ', time_taken)
for document in result:
    print(document)

# find the stores that had the highest total sales in USD for each county
# tt = 2.8895318508148193
start_time = time.time()
pipeline = [
    {
        "$group": {
            "_id": {
                "county": "$county",
                "store_number": "$store_number"
            },
            "total_sales_usd": {"$sum": "$sale_usd"}
        }
    },
    {
        "$sort": {
            "_id.county": 1,
            "total_sales_usd": -1
        }
    },
    {
        "$group": {
            "_id": "$_id.county",
            "max_total_sales": {"$first": "$total_sales_usd"},
            "store": {"$first": "$_id.store_number"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "county": "$_id",
            "store_number": "$store",
            "total_sales_usd": "$max_total_sales"
        }
    },
    {
        "$sort": {
            "total_sales_usd": -1
        }
    }
]

result = list(ls.aggregate(pipeline))
end_time = time.time()
time_taken = end_time - start_time
for doc in result:
    print(doc)
print('Time taken: ', time_taken)