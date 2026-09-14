FROM node:20-slim AS builder
WORKDIR /src
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npm run build

# Compiled TS, not tsx -- npx tsx is a dev convenience for fast iteration,
# not how compiled TypeScript is run in a container.
FROM node:20-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --from=builder /src/dist ./dist
ENTRYPOINT ["node", "dist/index.js"]
