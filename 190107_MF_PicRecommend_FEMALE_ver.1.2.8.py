import pandas as pd

import pymysql

import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from os import listdir
from os.path import isfile, join

### Read rds at first
connection = pymysql.connect(host='codymonster-maria.cfnceagzudnn.ap-northeast-2.rds.amazonaws.com', port=3306,
                             user='comon', passwd='306crewcodymonster',
                             charset='utf8', autocommit=True)

cursor = connection.cursor()
rating = "SELECT * FROM ai.rating_transaction"
pic = "SELECT * FROM ai.pic_list"

cursor.execute(rating)
result = cursor.fetchall()
Rating_array = []
for rating in result:
    Rating_array.append(rating)

Rating_df = pd.DataFrame(Rating_array, columns=["Customer_ID", "Cloth_ID", "Rating"])
cursor.execute(pic)
result = cursor.fetchall()
Pic_array = []
for pic in result:
    Pic_array.append(pic)

Pic_df = pd.DataFrame(Pic_array, columns=["Cloth_ID", "Cloth_Name"])

for i in range(len(Pic_df)):
    Pic_df.iloc[i, 1] = Pic_df.iloc[i, 1].replace("\r", "")

cursor.close()

# predictions.to_csv('C:/Users/MINSU/181228_test.csv',encoding='utf8')
###############################################

HOST_NAME = '0.0.0.0'
PORT_NUMBER = 9000
content = ''

class MyHandler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        if content != None:
            self.respond({'status': 200})
        else:
            self.respond({'status': 404})

    def Pic_Recom(self, userID, Picture_df, Original_Rating_df, num_recommendations=10):
        files = [f for f in listdir('../Data/') if isfile(join('../Data/', f))]
        predictions_df = pd.read_csv('../Data/{}'.format(files[-1]))
        predictions_df.columns = list(map(int, predictions_df.columns))
        predictions_df.columns.name = 'Cloth_ID'

        if userID not in predictions_df.index:
            predictions_df.loc[userID, :] = 2.5 # 초기값
        else:
            pass

        # Get and sort the user's predictions
        user_row_number = userID - 1  # UserID starts at 1, not 0
        sorted_user_predictions = predictions_df.iloc[user_row_number].sort_values(ascending=False)
        # print(sorted_user_predictions)
        user_data = Original_Rating_df[Original_Rating_df.Customer_ID == (userID)]
        user_full = pd.merge(user_data, Picture_df, how='left', left_on='Cloth_ID', right_on='Cloth_ID').sort_values(
            ['Rating'], ascending=False)

        # Recommend the highest predicted rating movies that the user hasn't seen yet.
        recommendations = (Picture_df[~Picture_df['Cloth_ID'].isin(user_full['Cloth_ID'])].
                                merge(pd.DataFrame(sorted_user_predictions).reset_index(), how='left',
                                    left_on='Cloth_ID',
                                    right_on='Cloth_ID').
                            rename(columns={user_row_number: 'Predictions'}).
                            sort_values('Predictions', ascending=False).
                            iloc[:num_recommendations, :-1]
                            )

        return user_full, recommendations


    def handle_http(self, status_code, path):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        global Func
        global user_id

        if path != '/favicon.ico':
            Func = path[1:10]
            user_id = path[11:].encode('UTF-8')
            user_id = int(user_id)
        else:
            pass

        if Func == 'Pic_Recom':
            already_rated, predictions = self.Pic_Recom(user_id, Pic_df, Rating_df, 10)
            content = predictions.to_json()
        else:
            pass
        return bytes(content, 'UTF-8')


    def respond(self, opts):
        response = self.handle_http(opts['status'], self.path)
        self.wfile.write(response)

# get으로

if __name__ == '__main__':
    server_class = HTTPServer
    httpd = server_class((HOST_NAME, PORT_NUMBER), MyHandler)
    print(time.asctime(), 'Server Starts - %s:%s' % (HOST_NAME, PORT_NUMBER))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print(time.asctime(), 'Server Stops - %s:%s' % (HOST_NAME, PORT_NUMBER))


