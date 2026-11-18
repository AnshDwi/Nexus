# NEXUS dashboard for local / self-hosted use: a production build served by
# `vite preview`, which proxies /api to NEXUS_API_PROXY (the API service), so
# the browser talks to one origin and no CORS setup is needed.
# On Vercel the dashboard is a static deployment instead (frontend/vercel.json).
ARG NODE_IMAGE=node:22-alpine
FROM ${NODE_IMAGE}
WORKDIR /app
ENV NPM_CONFIG_UPDATE_NOTIFIER=false NPM_CONFIG_FUND=false NPM_CONFIG_AUDIT=false
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build
ENV NEXUS_API_PROXY=http://api:8000
USER 1000:1000
EXPOSE 5173
CMD ["node", "node_modules/vite/bin/vite.js", "preview", "--host", "0.0.0.0", "--port", "5173", "--strictPort"]
