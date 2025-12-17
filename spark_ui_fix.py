from flask import Flask, render_template, request, jsonify, redirect, url_for
import pandas as pd
import os
import json
import socket

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/spark-ui')
def spark_ui():
    # Check common Spark UI ports
    hostname = socket.gethostname()
    for port in [4040, 4041, 4042, 4043]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((hostname, port))
            sock.close()
            if result == 0:
                return redirect(f'http://{hostname}:{port}')
        except:
            continue
    # Fallback to the known Spark UI endpoint
    return redirect('http://MSI:4040')

@app.route('/test-spark-ui')
def test_spark_ui():
    """
    Test endpoint to verify Spark UI connectivity
    """
    # Try to connect to common Spark UI ports
    hostname = socket.gethostname()
    available_ports = []
    
    for port in [4040, 4041, 4042, 4043]:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((hostname, port))
            sock.close()
            if result == 0:
                available_ports.append(port)
        except Exception as e:
            print(f"Error checking port {port}: {e}")
    
    if available_ports:
        return jsonify({
            "status": "success",
            "available_ports": available_ports,
            "message": f"Spark UI available on ports: {available_ports}"
        })
    else:
        return jsonify({
            "status": "error",
            "available_ports": [],
            "message": "No Spark UI instances found on common ports"
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)