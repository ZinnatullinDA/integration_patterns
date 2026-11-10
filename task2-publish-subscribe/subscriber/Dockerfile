FROM node:24.22.1-alpine3.23
WORKDIR /app
COPY subscriber/package.json subscriber/package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY common/*.js ./
COPY subscriber/subscriber.js ./
USER node
CMD ["node", "subscriber.js"]

