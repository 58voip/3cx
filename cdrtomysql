import os
import socket
import threading
import logging
import mysql.connector
import re


# log detail
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tcpsocket.log')
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


def process_cdr_data(data):
    from_no, to_no, duration, time_end = data.strip().split(',')
    
    # 替换日期分隔符以匹配 MySQL DATETIME 格式
    if re.search(r'\d{4}/\d{2}/\d{2}', time_end):
        time_end = time_end.replace('/', '-')
    elif re.search(r'\d{4}-\d{2}-\d{2}', time_end):
        pass
    else:
        logging.warning(f"Invalid date format: {time_end}")
        return None

    return from_no, to_no, duration, time_end


def insert_cdr_data(cdr_data):
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="guang",
            password="@Qwer1234dear",
            database="CDR"
        )

        mycursor = mydb.cursor()
        sql = "INSERT INTO cdr (from_no, to_no, duration, time_end) VALUES (%s, %s, %s, %s)"
        mycursor.execute(sql, cdr_data)
        mydb.commit()
        mycursor.close()
        mydb.close()

        logging.info(f"Inserted CDR data into MySQL: {', '.join(cdr_data)}")
    except Exception as e:
        logging.error(f"Error inserting CDR data into MySQL: {e}")



def handle_client_connection(client_socket, client_address):
    try:
        client_socket.settimeout(5)
        data = client_socket.recv(1024)
        logging.info(f"Connected by {client_address}")

        if data:
            cdr_data = process_cdr_data(data.decode('utf-8'))
            logging.info(f"Received CDR data: {', '.join(cdr_data)}")
            insert_cdr_data(cdr_data)

        client_socket.close()
    except socket.timeout:
        logging.warning("Socket timeout")
        client_socket.close()


def listen_to_socket(host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen()

    logging.info(f"Listening on {host}:{port}")

    while True:
        try:
            client_sock, client_addr = server_socket.accept()
            client_thread = threading.Thread(target=handle_client_connection, args=(client_sock, client_addr))
            client_thread.start()
        except KeyboardInterrupt:
            break

    server_socket.close()


if __name__ == '__main__':
    listen_to_socket('0.0.0.0', 3000)
