"""
Notification Service API - Async with rq

This version sends notifications ASYNCHRONOUSLY using rq.
Each request returns instantly with a job_id.
"""

from flask import Flask, jsonify, request
from redis import Redis
from rq.job import Job
import os
import uuid
from tasks import send_notification

app = Flask(__name__)

redis_conn = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))

# In-memory store for notifications
notifications = {}


@app.route('/')
def index():
    return jsonify({
        "service": "Notification Service (Async with rq!)",
        "endpoints": {
            "POST /notifications": "Queue a notification (returns instantly!)",
            "GET /notifications": "List all notifications",
            "GET /notifications/<id>": "Get a notification",
            "GET /jobs/<job_id>": "Check job status"
        }
    })


@app.route('/notifications', methods=['POST'])
def create_notification():
    """
    Queue a notification for background processing.
    Returns immediately with a job_id.
    """
    data = request.get_json()

    if not data or 'email' not in data:
        return jsonify({"error": "Email is required"}), 400

    notification_id = str(uuid.uuid4())
    email = data['email']
    message = data.get('message', 'You have a new notification!')

    # Queue the task — returns instantly!
    job = send_notification.delay(notification_id, email, message)

    notification = {
        "id": notification_id,
        "email": email,
        "message": message,
        "status": "queued",
        "job_id": job.id
    }
    notifications[notification_id] = notification

    return jsonify(notification), 202


@app.route('/notifications', methods=['GET'])
def list_notifications():
    """List all notifications."""
    return jsonify({
        "notifications": list(notifications.values())
    })


@app.route('/notifications/<notification_id>', methods=['GET'])
def get_notification(notification_id):
    """Get a single notification."""
    notification = notifications.get(notification_id)
    if not notification:
        return jsonify({"error": "Notification not found"}), 404
    return jsonify(notification)


@app.route('/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Check the status of a background job."""
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        return jsonify({"error": "Job not found"}), 404

    status = job.get_status()
    response = {
        "job_id": job_id,
        "status": str(status)
    }

    if status.value == "finished" and job.result:
        response["result"] = job.result

    if status.value == "failed":
        response["error"] = str(job.exc_info)

    return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)