ExUnit.start()

# Setup Mox for global mocking
Application.put_env(:gust_k8s, :k8s_client, GustK8s.K8sClientMock)
