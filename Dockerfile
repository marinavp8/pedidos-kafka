# Una sola imagen parametrizada por servicio: docker build --build-arg SERVICE=pedidos .
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.base.json tsconfig.json ./
COPY libs ./libs
COPY services ./services
RUN npm ci && npm run build && npm prune --omit=dev

FROM node:22-alpine
ARG SERVICE
ENV NODE_ENV=production SERVICE=${SERVICE}
WORKDIR /app
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/libs ./libs
COPY --from=build /app/services/${SERVICE} ./services/${SERVICE}
USER node
CMD ["sh", "-c", "exec node services/$SERVICE/dist/index.js"]
