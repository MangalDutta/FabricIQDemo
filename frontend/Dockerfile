# Multi-stage build
FROM node:20-alpine AS builder

ARG VITE_BACKEND_URL=""
ARG VITE_POWERBI_REPORT_URL=""

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .
ENV VITE_BACKEND_URL=$VITE_BACKEND_URL
ENV VITE_POWERBI_REPORT_URL=$VITE_POWERBI_REPORT_URL
RUN npm run build

FROM node:20-alpine AS production

WORKDIR /app
RUN npm install -g serve

COPY --from=builder /app/dist ./dist

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD sh -c "wget -qO- http://localhost:${PORT:-8080}/ > /dev/null 2>&1 || exit 1"

CMD ["sh", "-c", "serve -s dist -l ${PORT:-8080}"]
