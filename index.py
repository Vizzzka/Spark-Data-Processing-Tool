import pyspark
import os
from pyspark.sql import SparkSession
import sys
import json
import datetime
import functools
from calendar import monthrange


def date_comparator(date):
    date = date.split('.')
    return [date[0] + date[2] + date[1]].join('')


def month_by_day(day):
    year, day, month = map(int, day.split('.'))
    year += 2000
    date = datetime.date(year, month, day)
    start_date = datetime.date(year, month, 1)
    end_date = datetime.date(year, month, monthrange(year, month)[1])

    return start_date.strftime("%Y.%d.%m"), end_date.strftime("%Y.%d.%m")


def week_by_day(day):
    year, day, month = map(int, day.split('.'))
    year += 2000
    date = datetime.date(year, month, day)
    start_date = date - datetime.timedelta(days=date.weekday())
    end_date = date + datetime.timedelta(days=6 - date.weekday())

    return start_date.strftime("%Y.%d.%m"), end_date.strftime("%Y.%d.%m")


def first_query(data):
    data = data.map(lambda x: ((x[0], x[2], x[15]), [(x[1], x[7], x[8], x[9])]))\
        .reduceByKey(lambda a, b: a + b)\
        .sortBy(lambda x: -len(x[1]))\
        .take(10)

    report = {"videos": []}
    for video in data:
        (id, title, description), dates = video
        dates = sorted(dates, key=lambda x: date_comparator(x[0]), reverse=True)
        _, latest_views, latest_likes, latest_dislikes = dates[0]
        dates = list(map(lambda x: {"date": x[0], "views": x[1], "likes": x[2], "dislikes": x[3]}, dates))
        video = {"id": id, "title": title, "description": description, "latest_views": latest_views,
                 "latest_likes": latest_likes, "latest_dislikes": latest_dislikes, "trending_days": dates}
        report["videos"].append(video)

    return report


def second_query(data, categories):
    data = data.map(lambda x: ((x[4], x[0], week_by_day(x[1])), [int(x[7])]))\
        .reduceByKey(lambda a, b: a + b)\
        .filter(lambda x: len(x[1]) > 1)\
        .map(lambda el: (el[0], sorted(el[1])))\
        .map(lambda el: (el[0], -el[1][0] + functools.reduce(lambda a, b: a + b, el[1][1:])))\
        .map(lambda el: ((el[0][0], el[0][2]), ([el[0][1]], el[1])))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .map(lambda el: (el[0][1], (el[0][0], el[1][0], el[1][1])))\
        .reduceByKey(lambda a, b: a if a[2] > b[2] else b)\
        .collect()

    report = {"weeks": []}
    for week in data:
        start_date = week[0][0]
        end_date = week[0][1]
        category_id = week[1][0]
        category_name = list(filter(lambda x: x["id"] == str(category_id), categories["items"]))[0]["snippet"]["title"]
        number_of_videos = len(week[1][0])
        total_views = week[1][1]
        video_ids = week[1][0]
        week = {"start_date": start_date, "end_date": end_date, "category_id": category_id,
                "category_name": category_name, "number_of_videos": number_of_videos,
                "total_views": total_views, "video_ids": video_ids}
        report["weeks"].append(week)

    return report


def third_query(data):
    data = data.map(lambda x: ((month_by_day(x[1]), x[0]), (x[6].split('|'))))\
        .reduceByKey(lambda a, b: a)\
        .flatMapValues(lambda x: x) \
        .filter(lambda x: x[1] != '[none]') \
        .map(lambda x: ((x[0][0], x[1]), [x[0][1]]))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[0][0], [(x[0][1], x[1])]))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[0], sorted(x[1], key=lambda val: -len(val[1]))[:10]))\
        .collect()

    report = {"months": []}
    for month in data:
        start_date = month[0][0]
        end_date = month[0][1]
        top_tags = map(lambda x: {"tag": x[0], "number_of_videos": len(x[1]), "video_ids": x[1]}, month[1])
        month = {"start_date": start_date, "end_date": end_date, "tags": top_tags}

        report["months"].append(month)

    return data


def fourth_query(data):
    data = data.map(lambda x: ((x[3], x[0]), [(int(x[7]), x[1])]))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda el: (el[0], (max(el[1])[0], min(el[1])[1], max(el[1])[1])))\
        .map(lambda el: (el[0][0], ([(el[0][1], el[1][0])], el[1][0], el[1][1], el[1][2])))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], min(a[2], b[2]), max(a[3], b[3])))\
        .sortBy(lambda x: -x[1][1])\
        .take(20)

    report = {"channels": []}

    for channel in data:
        channel_name = channel[0]
        start_date = channel[1][2]
        end_date = channel[1][3]
        total_views = channel[1][1]
        videos_views = list(map(lambda x: {"video_id": x[0], "views": x[1]}, channel[1][0]))
        channel = {"channel_name": channel_name, "start_date": start_date, "end_date": end_date,
                   "total_views": total_views, "videos_views": videos_views}

        report["channels"].append(channel)

    return report


def fifth_query(data):
    data = data.map(lambda x: ((x[0], x[2], x[3]), [x[1]]))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[0][2], [(x[0][0], x[0][1], len(x[1]))]))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[0], (sum(list(map(lambda val: val[2], x[1]))), x[1])))\
        .sortBy(lambda x: -x[1][0])\
        .take(10)

    report = {"channels": []}
    for channel in data:
        channel_name = channel[0]
        total_trending_days = channel[1][0]
        videos_days = list(map(lambda x: {
            "video_id": x[0], "video_title": x[1], "trending_days": x[2]
            }, channel[1][1]))
        channel = {"channel_name": channel_name, "total_trending_days": total_trending_days,
                   "videos_days": videos_days}
        report["channels"].append(channel)

    return data


def store_report(outputPath, report_id, region, report):
    json_report = json.dumps(report)
    outputPath = outputPath + "copycat_inc/" + report_id + "/" + region + "/result.json"
    os.makedirs(os.path.dirname(outputPath), exist_ok=True)
    with open(outputPath, 'w', encoding='ascii') as f:
        json.dump(json_report, f, indent=4)


if __name__ == "__main__":
    sc = pyspark.SparkContext('local[*]')
    ss = SparkSession.builder.appName("to read csv file").getOrCreate()
    sc.setLogLevel("ERROR")

    inputPath = sys.argv[1]
    outputPath = sys.argv[2]

    regions = {"CA": None, "DE": None, "FR": None, "GB": None, "IN": None,
               "JP": None, "KR": None, "MX": None, "RU": None, "US": None}

    for key, value in regions.items():
        regions[key] = ss.read.option("multiline", True)\
            .csv(inputPath + key + "videos" + ".csv", header=True)\
            .rdd
        with open(inputPath + key + "_category_id.json", encoding="ascii") as json_file:
            categories = json.load(json_file)

        report_1 = first_query(regions[key])
        print(report_1)
        store_report(outputPath, "1", key, report_1)

        report_2 = second_query(regions[key], categories)
        print(report_2)
        store_report(outputPath, "2", key, report_2)

        report_4 = fourth_query(regions[key])
        print(report_4)
        store_report(outputPath, "4", key, report_4)

        report_3 = third_query(regions[key])
        print(report_3)
        store_report(outputPath, "3", key, report_3)

        report_5 = fifth_query(regions[key])
        print(report_5)
        store_report(outputPath, "5", key, report_5)

