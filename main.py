
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
     [POST]200square-to-zoho-crm.onrender.com/square/webhookclientIP="54.245.1.154" requestID="c744c4f0-fc24-4f1e" responseTimeMS=1081 responseBytes=235 userAgent="Square Connect v2"
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
     [POST]200square-to-zoho-crm.onrender.com/square/webhookclientIP="54.245.1.154" requestID="0a9457c3-5755-4ec3" responseTimeMS=1002 responseBytes=235 userAgent="Square Connect v2"
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
     [POST]200square-to-zoho-crm.onrender.com/square/webhookclientIP="54.245.1.154" requestID="cf2be4f6-4897-494a" responseTimeMS=1078 responseBytes=235 userAgent="Square Connect v2"
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
     [POST]200square-to-zoho-crm.onrender.com/square/webhookclientIP="54.245.1.154" requestID="8eab5e32-c1b1-4019" responseTimeMS=805 responseBytes=235 userAgent="Square Connect v2"
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
     [POST]200square-to-zoho-crm.onrender.com/square/webhookclientIP="54.245.1.154" requestID="c947c157-ff82-477e" responseTimeMS=1109 responseBytes=235 userAgent="Square Connect v2"
     [POST]200square-to-zoho-crm.onrender.com/square/webhookclientIP="54.245.1.154" requestID="1c3158dd-6b34-41cb" responseTimeMS=4 responseBytes=227 userAgent="Square Connect v2"
     [POST]200square-to-zoho-crm.onrender.com/square/webhookclientIP="54.245.1.154" requestID="ff4b4195-e892-4369" responseTimeMS=1845 responseBytes=390 userAgent="Square Connect v2"
     [POST]200square-to-zoho-crm.onrender.com/square/webhookclientIP="54.245.1.154" requestID="78ba0574-f876-44a2" responseTimeMS=313 responseBytes=227 userAgent="Square Connect v2"
     [POST]500square-to-zoho-crm.onrender.com/square/webhookclientIP="54.245.1.154" requestID="e21623a4-e15a-4903" responseTimeMS=2090 responseBytes=227 userAgent="Square Connect v2"
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 500 Internal Server Error
6542w
ERROR:    Exception in ASGI application
6542w
Traceback (most recent call last):
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/uvicorn/protocols/http/h11_impl.py", line 403, in run_asgi
6542w
    result = await app(  # type: ignore[func-returns-value]
6542w
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/uvicorn/middleware/proxy_headers.py", line 60, in __call__
6542w
    return await self.app(scope, receive, send)
6542w
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/fastapi/applications.py", line 1054, in __call__
6542w
    await super().__call__(scope, receive, send)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/applications.py", line 112, in __call__
6542w
    await self.middleware_stack(scope, receive, send)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/middleware/errors.py", line 187, in __call__
6542w
    raise exc
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/middleware/errors.py", line 165, in __call__
6542w
    await self.app(scope, receive, _send)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/middleware/exceptions.py", line 62, in __call__
6542w
    await wrap_app_handling_exceptions(self.app, conn)(scope, receive, send)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/_exception_handler.py", line 53, in wrapped_app
6542w
    raise exc
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/_exception_handler.py", line 42, in wrapped_app
6542w
    await app(scope, receive, sender)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/routing.py", line 714, in __call__
6542w
    await self.middleware_stack(scope, receive, send)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/routing.py", line 734, in app
6542w
    await route.handle(scope, receive, send)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/routing.py", line 288, in handle
6542w
    await self.app(scope, receive, send)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/routing.py", line 76, in app
6542w
    await wrap_app_handling_exceptions(app, request)(scope, receive, send)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/_exception_handler.py", line 53, in wrapped_app
6542w
    raise exc
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/_exception_handler.py", line 42, in wrapped_app
6542w
    await app(scope, receive, sender)
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/starlette/routing.py", line 73, in app
6542w
    response = await f(request)
6542w
               ^^^^^^^^^^^^^^^^
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/fastapi/routing.py", line 301, in app
6542w
    raw_response = await run_endpoint_function(
6542w
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
6542w
  File "/opt/render/project/src/.venv/lib/python3.11/site-packages/fastapi/routing.py", line 212, in run_endpoint_function
6542w
    return await dependant.call(**values)
6542w
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
6542w
  File "/opt/render/project/src/main.py", line 65, in square_webhook
6542w
    zhdr = {"Authorization": f"Zoho-oauthtoken {zoho_access_token()}",
6542w
                                                ^^^^^^^^^^^^^^^^^^^
6542w
  File "/opt/render/project/src/main.py", line 28, in zoho_access_token
6542w
    raise RuntimeError(f"Zoho token error: {r}")
6542w
RuntimeError: Zoho token error: {'error_description': 'You have made too many requests continuously. Please try again after some time.', 'error': 'Access Denied', 'status': 'failure'}
6542w
INFO:     54.245.1.154:0 - "POST /square/webhook HTTP/1.1" 200 OK
     ==> Detected service running on port 10000
     ==> Docs on specifying a port: https://render.com/docs/web-services#port-binding
