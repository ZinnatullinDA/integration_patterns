FROM node:24.22.1-alpine3.23
WORKDIR /app
COPY publisher/package.json publisher/package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY common/*.js ./
COPY publisher/publisher.js ./
USER node
CMD ["node", "publisher.js"]

