FROM node:14-alpine
WORKDIR /app

COPY package*.json ./
RUN npm install

# Копируем ВСЕ файлы, включая posts.proto и app.js
COPY . .

EXPOSE 3000
CMD ["npm", "start"]