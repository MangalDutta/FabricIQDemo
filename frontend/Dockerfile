# Multi-stage build
FROM node:20-alpine AS builder

# VITE_BACKEND_URL and VITE_POWERBI_REPORT_URL are injected via .env.production
# written into the build context by the CI workflow before calling az acr build.
# This avoids passing --build-arg values with '&' through the ACR task runner's
# unquoted /bin/sh shell, which caused ".: filename argument required" failures.

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build

FROM node:20-alpine AS production

WORKDIR /app
RUN npm install -g serve

COPY --from=builder /app/dist ./dist

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD sh -c "wget -qO- http://localhost:${PORT:-8080}/ > /dev/null 2>&1 || exit 1"

CMD ["sh", "-c", "serve -s dist -l ${PORT:-8080}"]
