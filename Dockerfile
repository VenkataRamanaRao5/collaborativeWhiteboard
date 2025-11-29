FROM node:20

# Create app directory (Cleaner to use /app)
WORKDIR /app

# Install app dependencies
COPY package*.json ./
RUN npm install

# Bundle app source
COPY . .

EXPOSE 8080

# Run application
CMD [ "node", "index.js" ]