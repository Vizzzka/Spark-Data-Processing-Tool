# Spark-Data-Processing-Tool
Reporting tool which analyzes trending video on YouTube for the last few years.

This is the proof of concept (PoC) for the company CopyCat.inc. The company specializes in copycatting the most popular videos from YouTube and releasing them on their own YouTube channel. For now, they have dedicated people to watch videos and find trending candidates. However, the board of directors decided to experiment with the automation of this work. In order to know which videos to imitate they ordered you to build a special reporting tool. The tool should be using the scraped statistics about the trending videos from Youtube. Company came up with 6 use cases for the reporting tool. 

[Dataset link](https://www.kaggle.com/datasnaek/youtube-new)

**Reporting functionality:**

1. Find Top 10 videos that were amongst the trending videos for the highest number of days.
2. Find what was the most popular category for each week . Popularity is decided based on the total number of views for videos of this category.
3. Find what were the 10 most used tags amongst trending videos for each 30days time period
4. Show the top 20 channels by the number of views for the whole period.
5. Show the top 10 channels with videos trending for the highest number of days
6. Show the top 10 videos by the ratio of likes/dislikes for each category for the whole period. You should consider only videos with more than 100K views.

**Design document:**
ll

**Example:**
First query report for CAvideos.csv:

```
{
    "videos": [
        {
            "id": "6ZfuNTqbHE8",
            "title": "Marvel Studios' Avengers: Infinity War Official Trailer",
            "description": "There was an idea\u2026 Avengers: Infinity War. In theaters May 4.\\n\\n\u25ba Subscribe to Marvel: http://bit.ly/WeO3YJ\\n\\nFollow Marvel on Twitter: \u202ahttps://twitter.com/marvel\\nLike Marvel on FaceBook: \u202ahttps://www.facebook.com/Marvel\\n\\nFor even more news, stay tuned to:\\nTumblr: \u202ahttp://marvelentertainment.tumblr.com/\\nInstagram: https://www.instagram.com/marvel\\nGoogle+: \u202ahttps://plus.google.com/+marvel\\nPinterest: \u202ahttp://pinterest.com/marvelofficial",
            "latest_views": "89930713",
            "latest_likes": "2606665",
            "latest_dislikes": "53011",
            "trending_days": [
                {
                    "date": "17.07.12",
                    "views": "89930713",
                    "likes": "2606665",
                    "dislikes": "53011"
                },
                ...
                ...
                ...
                {
                    "date": "17.30.11",
                    "views": "37736281",
                    "likes": "1735931",
                    "dislikes": "21972"
                }
            ]
        },

```


**Link to data stored on gcp:**
https://console.cloud.google.com/storage/browser/ucu-hw-3

Processing performance measurements file contains info about:
- Size of the input file
- Size of the resulting data
- Time of processing for each question
- Cluster params (RAM, CPU cores, number of nodes)
