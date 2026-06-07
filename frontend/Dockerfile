# Stage 1: Build Angular production bundle
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci --legacy-peer-deps
COPY . .
RUN npm run build -- --configuration production

# Stage 2: Serve with Nginx (also acts as reverse proxy for /api/)
FROM nginx:alpine
COPY --from=builder /app/dist/frontend /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
