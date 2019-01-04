import pandas as pd

print(pd.__version__)
import numpy as np
import pymysql
import http.server

import time
from http.server import BaseHTTPRequestHandler, HTTPServer

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

print("Rating_df's shape: ", Rating_df.shape)
print("Pic_df's shape: ", Pic_df.shape)


# Rating_df = pd.read_csv("C:/Users/Min-su/Pic_Recom/Rating_Transaction.csv") 로컬에서 불러올 때
# Pic_df = pd.read_csv("C:/Users/Min-su/Pic_Recom/Pic_List.csv") 로컬에서 불러올 때

# ### 트랜잭션 데이터 피봇화
def pivot_table_dataframe(dataframe):
    pivot_dataframe = pd.pivot_table(dataframe, values='Rating', index='Customer_ID', columns='Cloth_ID')
    pivot_dataframe_fill = pivot_dataframe.fillna(0)
    return (pivot_dataframe_fill)


print(Rating_df.tail())
R_df = pivot_table_dataframe(Rating_df)
# while R_df.shape != (39, 52):
#     R_df = pivot_table_dataframe(Rating_df)

# R_df = pd.pivot_table(Rating_df, values='Rating', index='Customer_ID', columns='Cloth_ID')
# R_df = R_df.fillna(0)

print("R_df's shape: ", R_df.shape)

R = R_df.values


def mean_not_zero(matrix):
    user_ratings_mean = np.zeros(matrix.shape[0])
    for i in range(0, matrix.shape[0]):
        sum_ = 0
        count = 0
        for j in range(0, matrix.shape[1]):  # 열
            if matrix[i][j] != 0:
                sum_ = sum_ + matrix[i][j]
                count = count + 1
            else:
                matrix[i][j] = 0
        user_ratings_mean[i] = sum_ / count
        for k in range(0, matrix.shape[1]):
            if matrix[i][k] == 0:
                matrix[i][k] = user_ratings_mean[i]
            else:
                pass
    return matrix, user_ratings_mean


R_demeaned_pro, user_ratings_mean = mean_not_zero(R)
user_ratings_mean_pro = np.mean(R_demeaned_pro, axis=1)
R_demeaned = R_demeaned_pro - user_ratings_mean_pro.reshape(-1, 1)  # .reshape(-1,1) = 결과값 행을 열로 변경

R_demeaned = pd.DataFrame(R_demeaned)
print(R_demeaned.shape)
from scipy.sparse.linalg import svds

U, sigma, Vt = svds(R_demeaned, k=10)
sigma = np.diag(sigma)
all_user_predicted_ratings = np.dot(np.dot(U, sigma), Vt) + user_ratings_mean.reshape(-1, 1)
preds_df = pd.DataFrame(all_user_predicted_ratings, columns=R_df.columns)


def recommend_movies(predictions_df, userID, Picture_df, Original_Rating_df, num_recommendations=10):
    # Get and sort the user's predictions
    user_row_number = userID - 1  # UserID starts at 1, not 0
    sorted_user_predictions = predictions_df.iloc[user_row_number].sort_values(ascending=False)
    # print(sorted_user_predictions)
    # Get the user's data and merge in the movie information.
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


already_rated, predictions = recommend_movies(preds_df, 33, Pic_df, Rating_df, 10)

content = predictions.to_json()
# predictions.to_csv('C:/Users/MINSU/181228_test.csv',encoding='utf8')
###############################################

HOST_NAME = '0.0.0.0'
PORT_NUMBER = 9000


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

    def handle_http(self, status_code, path):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        return bytes(content, 'UTF-8')

    def respond(self, opts):
        response = self.handle_http(opts['status'], self.path)
        self.wfile.write(response)


# 포스트로 받는 부분만 만들어서 동작부분만 집어 넣으면 된다.

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
