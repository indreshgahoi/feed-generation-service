# Pure static file serving -- Envoy now owns all API routing (see
# doc/gateway.md), so this container no longer needs serve.py's
# hand-rolled reverse-proxy logic. serve.py itself is left in place for
# quick host-only iteration without Docker.
FROM nginx:alpine
COPY index.html app.js styles.css /usr/share/nginx/html/
EXPOSE 80
