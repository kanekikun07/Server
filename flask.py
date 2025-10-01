# Flask example
from flask import Flask, send_file
app = Flask(__name__)

@app.route('/download')
def download_file():
    return send_file('user_proxies.txt', as_attachment=True)
