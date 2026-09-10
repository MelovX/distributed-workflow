FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["DistributedWorkflow.KafkaOutboxPublisher/DistributedWorkflow.KafkaOutboxPublisher.csproj", "DistributedWorkflow.KafkaOutboxPublisher/"]
RUN dotnet restore "DistributedWorkflow.KafkaOutboxPublisher/DistributedWorkflow.KafkaOutboxPublisher.csproj"

COPY . .
WORKDIR "/src/DistributedWorkflow.KafkaOutboxPublisher"
RUN dotnet publish "DistributedWorkflow.KafkaOutboxPublisher.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 8080

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "DistributedWorkflow.KafkaOutboxPublisher.dll"]
