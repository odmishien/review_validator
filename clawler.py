from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import pymysql.cursors

def get_genre_page_uri():
    genre_uris = []
    top_page = urlopen("https://itunes.apple.com/jp/genre/ios/id36?mt=8")
    soup = BeautifulSoup(top_page, 'html.parser')
    genre_list_sections = soup.find_all('ul', 'list')
    for genre_list_section in genre_list_sections:
        genre_sections = genre_list_section.find_all('li')
        for genre_section in genre_sections:
            genre_uri = genre_section.find('a').get('href')
            genre_uris.append(genre_uri)
    return genre_uris

def get_app_ids(genre_uris):
    app_ids = []
    for genre_uri in genre_uris:
        genre_page = urlopen(genre_uri)
        soup = BeautifulSoup(genre_page, 'html.parser')
        app_list_sections = soup.find_all('div', 'column')
        for app_list_section in app_list_sections:
            try:
                app_sections = app_list_section.find_all('li')
                for app_section in app_sections:
                    app_uri = app_section.find('a').get('href')
                    pattern = re.compile(r'.*id(?P<id>\d+)')
                    result = pattern.search(app_uri)
                    app_id = result.group('id')
                    app_ids.append(app_id)
            except:
                print("failed")
    return app_ids

def get_reviews(app_id):
    reviews = []
    for page in range(1,11):
        xml_page = urlopen("https://itunes.apple.com/jp/rss/customerreviews/page={page}/id={app_id}/xml".format(page=page,app_id=app_id))
        soup = BeautifulSoup(xml_page, 'lxml-xml')
        reviews_sections = soup.find_all('entry')
        for review_section in reviews_sections:
            review = {}
            review["app_id"] = app_id
            pattern = re.compile(r'(.*)(\-07:00)')
            updated = pattern.search(review_section.find('updated').string)
            review["updated"] = updated.group(1)
            review["title"] = review_section.find('title').string
            review["content"] = review_section.find('content').string
            review["vote_sum"] = review_section.find('im:voteSum').string
            review["vote_count"] = review_section.find('im:voteSum').string
            review["rating"] = review_section.find('im:rating').string
            review["version"] = review_section.find('im:version').string
            reviews.append(review)
    return reviews

def insert_row(review):

# | reviews | CREATE TABLE `reviews` (
#   `id` int(11) NOT NULL AUTO_INCREMENT,
#   `updated` datetime DEFAULT NULL,
#   `title` tinytext,
#   `content` text,
#   `vote_sum` tinyint(4) DEFAULT NULL,
#   `vote_count` tinyint(4) DEFAULT NULL,
#   `rating` tinyint(4) DEFAULT NULL,
#   `version` tinytext,
#   `app_id` int(11) DEFAULT NULL,
#   PRIMARY KEY (`id`)
# ) ENGINE=InnoDB AUTO_INCREMENT=19259 DEFAULT CHARSET=utf8 |

    conn = pymysql.connect(host='localhost',
                    user='root',
                    db='review_validator',
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO reviews VALUES (0, %(updated)s, %(title)s, %(content)s, %(vote_sum)s, %(vote_count)s, %(rating)s, %(version)s, %(app_id)s)", {'updated': review["updated"], 'title': review["title"], 'content': review["content"], 'vote_sum': review["vote_sum"], 'vote_count': review["vote_count"], 'rating': review["rating"], 'version': review["version"], 'app_id': review["app_id"]})
            print(cursor._executed)
        conn.commit()
    except Exception as e:
        print(e)
    conn.close()

if __name__ == "__main__":
    genre_uris = get_genre_page_uri()
    app_ids = get_app_ids(genre_uris)
    for app_id in app_ids:
        reviews = get_reviews(app_id)
        for review in reviews:
            insert_row(review)
