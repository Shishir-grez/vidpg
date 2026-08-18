# VidPG browser client

The client is dependency-free static HTML and JavaScript. Start the FastAPI
relay on port `8000`, then serve this directory from a secure browser context:

```text
python -m http.server 8080 --directory web
```

Open `http://localhost:8080/?relay=http://localhost:8000/`. Create a session
in one browser, copy the peer link, and open it in the second browser. The
session capability remains in the URL fragment and is never sent as a query
parameter to the relay.
