from flask import Flask, redirect, url_for, request

app = Flask(__name__)


@app.route("/trigger_report")
def trigger_report():
    pass


@app.route("/get_report/<report_id>", methods=["GET"])
def get_report(report_id):
    pass


if __name__ == "__main__":
    app.run(debug=True)
