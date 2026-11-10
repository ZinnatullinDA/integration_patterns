FROM node:24.22.1-alpine3.23
WORKDIR /app
COPY producer/package.json producer/package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY common/*.js ./
COPY producer/producer.js ./
USER node
CMD ["node", "producer.js"]

