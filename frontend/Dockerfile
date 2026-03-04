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

EXPOSE $PORT

HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://localhost:$PORT/ > /dev/null 2>&1 || exit 1

CMD ["sh", "-c", "serve -s dist -l $PORT --no-clipboard"]