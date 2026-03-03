# Multi-stage build
FROM node:20-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production=false

COPY . .
RUN npm run build

# Production stage with serve
FROM node:20-alpine AS production

WORKDIR /app
RUN npm install -g serve

# Copy built assets
COPY --from=builder /app/dist ./dist

# Expose PORT (App Service sets this)
EXPOSE $PORT

# Health check
HEALTHCHECK --interval=30s --timeout=3s CMD node -e \\
  "require('http').get('http://localhost:'+process.env.PORT+'/favicon.ico',{timeout:2500},(r)=>r.statusCode==200).on('error',()=>process.exit(1))"

# Serve on $PORT (App Service injects this)
CMD ["sh", "-c", "serve -s dist -l $PORT --no-clipboard"]
