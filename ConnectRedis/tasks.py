from celery import Celery

app = Celery('tasks', broker='pyamqp://localhost')

@app.task
def add(x, y):
    print('here')
    return x + y