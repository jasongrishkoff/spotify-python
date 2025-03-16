preload_app = True
workers = 6
worker_class = 'uvicorn.workers.UvicornWorker'
bind = '0.0.0.0:8000'
timeout = 120
max_requests = 5000  
max_requests_jitter = 100
