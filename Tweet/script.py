import sqlite3

connection = sqlite3.connect('twitterdata.db')

print(
    "opened database successfully"
)

connection.execute('''create table twitter(time varchar(20),username varchar(50),tweet varchar(500),location varchar(20),hashtag varchar(20));''')


print ('Table created')
connection.commit()
connection.close()