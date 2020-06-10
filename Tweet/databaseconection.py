import sqlite3

class Connection:
    """Database connection Class"""
    def create_connection(self):
        """
        Creating connection to the database
        :return: conn
        """
        conn = None
        try:
            conn = sqlite3.connect("twitterdata.db")
        except ValueError as e:
            print(e)
        return conn