FROM node:14.21-alpine3.17
WORKDIR /app

COPY package*.json ./
RUN npm install

# Копируем ВСЕ файлы, включая posts.proto и app.js
COPY . .

EXPOSE 3000
CMD ["npm", "start"]