# pgfire
Convert your postgresql database to a Realtime NoSql database accessible via REST APIs. Inspired from Firebase.

## Setup Instructions

- git clone this repo
- `pip install -r requirements.txt`
- update config.json with db host, username and password
- run `python app.py`

## Demo

- Create a DB
```
curl --location --request POST 'http://localhost:8666/createdb' \
--header 'Content-Type: application/json' \
--data-raw '{
	"db_name":"test_nosql_db"
}'
```

- Write Data
Add a blog entry to path `blog`
```
curl --location --request PUT 'http://localhost:8666/database/test_nosql_db/blog' \
--header 'Content-Type: application/json' \
--data-raw '{
	"title":"a blog entry",
	"body": "some blah blah..."
}'
```

Add another blog entry
```
curl --location --request PUT 'http://localhost:8666/database/test_nosql_db/blog' \
--header 'Content-Type: application/json' \
--data-raw '{
	"title":"another blog entry",
	"body": "some more blah blah..."
}'
```

Read at path
```
curl --location --request GET 'http://localhost:8666/database/test_nosql_db/blog'
{
    "-M8eTWMpriELfoWJ0osW": {
        "body": "some blah blah...",
        "title": "a blog entry"
    },
    "-M8eUE4Yt004TQHQkjpG": {
        "body": "some more blah blah...",
        "title": "another blog entry"
    }
}
```
Read at root
```
curl --location --request GET 'http://localhost:8666/database/test_nosql_db/'
{
    "blog": {
        "-M8eTWMpriELfoWJ0osW": {
            "body": "some blah blah...",
            "title": "a blog entry"
        },
        "-M8eUE4Yt004TQHQkjpG": {
            "body": "some more blah blah...",
            "title": "another blog entry"
        }
    }
}
```

