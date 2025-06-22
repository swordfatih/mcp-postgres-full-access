FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./
COPY tsconfig.json ./
COPY src ./src

# Install dependencies
ENV NPM_CONFIG_IGNORE_SCRIPTS=true
RUN npm ci

# Copy source code
COPY tsconfig.json ./
COPY src ./src

# Build the application
RUN npm run build

FROM node:20-alpine AS release

WORKDIR /app

# Set to production environment
ENV NODE_ENV=production

# Copy package files and install production dependencies
COPY package*.json ./
COPY tsconfig.json ./
COPY src ./src

ENV NPM_CONFIG_IGNORE_SCRIPTS=true
RUN npm ci

# Copy built application from builder stage
COPY --from=builder /app/dist ./dist

# Create a non-root user and set permissions
RUN addgroup -S mcp && \
    adduser -S mcp -G mcp && \
    chown -R mcp:mcp /app

# Switch to non-root user
USER mcp

# Set the entrypoint
ENTRYPOINT ["node", "dist/index.js"]