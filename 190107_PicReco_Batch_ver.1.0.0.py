import pandas as pd
import numpy as np
import pymysql
import sys
import key

def pred_batch():
    connection = pymysql.connect(host=key.host, port=key.port,
                                 user=key.user, passwd=key.passwd,
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
    cursor.close()

    print("Rating_df's shape: ", Rating_df.shape)

    # ### 트랜잭션 데이터 피봇화
    def pivot_table_dataframe(dataframe):
        pivot_dataframe = pd.pivot_table(dataframe, values='Rating', index='Customer_ID', columns='Cloth_ID')
        pivot_dataframe_fill = pivot_dataframe.fillna(0)
        return (pivot_dataframe_fill)

    print(Rating_df.tail())
    R_df = pivot_table_dataframe(Rating_df)

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
    print("R_demeaned's shape: ", R_demeaned.shape)
    from scipy.sparse.linalg import svds

    U, sigma, Vt = svds(R_demeaned, k=10)
    sigma = np.diag(sigma)
    all_user_predicted_ratings = np.dot(np.dot(U, sigma), Vt) + user_ratings_mean.reshape(-1, 1)
    preds_df = pd.DataFrame(all_user_predicted_ratings, columns=R_df.columns)

    today = pd.Timestamp('today')
    preds_df.to_csv('../Data/preds_df_{:%Y%m%d%H%M}.csv'.format(today) ,index=False)

if __name__ == "__main__":
    pred_batch()
    sys.exit(1)
