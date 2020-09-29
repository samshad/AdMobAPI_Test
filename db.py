import mysql.connector


class MySql:
    def __init__(self):
        self.db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="root",
            database="testdb"
        )
        self.mycursor = self.db.cursor()

    def create_table(self):
        sql = "CREATE TABLE IF NOT EXISTS test (date DATE, app VARCHAR(100), country VARCHAR(20), " \
              "platform VARCHAR(15), ad_format VARCHAR(20), ad_unit VARCHAR(50), estimated_earnings DOUBLE, " \
              "clicks int, impressions int, match_request int, match_rate DOUBLE, show_rate DOUBLE, " \
              "ad_network VARCHAR(20), id int PRIMARY KEY AUTO_INCREMENT)"
        self.mycursor.execute(sql)

    def insert_data(self, data):
        sql = "INSERT INTO test (date, app, country, platform, ad_format, ad_unit, estimated_earnings, clicks, " \
                                "impressions, match_request, match_rate, show_rate, ad_network) " \
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.mycursor.execute(sql, data)
        self.db.commit()

    def print_table(self):
        sql = "SELECT * FROM test"
        self.mycursor.execute(sql)
        for i in self.mycursor:
            print(i)

    def drop_table(self):
        sql = "DROP TABLE test"
        self.mycursor.execute(sql)
        self.db.commit()
