from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import json
import time
from scheduler import scheduler

app = Flask(__name__)
CORS(app)

@app.route("/api/start", methods=["POST", "OPTIONS"])
def start():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    data = request.get_json(silent=True) or {}
    algorithm = data.get("algorithm", "FCFS")
    if algorithm not in scheduler.ALGORITHMS:
        return jsonify({"error": "Unknown algorithm"}), 400
    scheduler.start(algorithm)
    return jsonify({"status": "started", "algorithm": algorithm})

@app.route("/api/stop", methods=["POST", "OPTIONS"])
def stop():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    scheduler.stop()
    return jsonify({"status": "stopped"})

@app.route("/api/state", methods=["GET"])
def state():
    return jsonify(scheduler.get_state())

@app.route("/api/algorithms", methods=["GET"])
def algorithms():
    return jsonify({"algorithms": scheduler.ALGORITHMS})

@app.route("/api/stream")
def stream():
    def event_gen():
        while True:
            s = scheduler.get_state()
            yield f"data: {json.dumps(s)}\n\n"
            time.sleep(0.5)
    return Response(event_gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

if __name__ == "__main__":
    print("Backend running on http://localhost:5001")
    app.run(debug=False, host="0.0.0.0", port=5001, threaded=True)