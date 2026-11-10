FROM node:24.22.1-alpine3.23
WORKDIR /app
COPY consumer/package.json consumer/package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY common/*.js ./
COPY consumer/consumer.js ./
USER node
CMD ["node", "consumer.js"]

