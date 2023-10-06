from locust import HttpUser, task

class HelloWorldUser(HttpUser):
    @task
    def run_query(self):
        self.client.get("/query")
