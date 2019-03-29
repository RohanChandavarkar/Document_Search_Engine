import sys
from flask import Flask, Response, request

app = Flask(__name__)


@app.route('/')
def display_data():
    query = sys.argv[1]

    return 'The user query is: {}'.format(query)

if __name__ == '__main__':
    app.run(host = '0.0.0.0',debug=True)
