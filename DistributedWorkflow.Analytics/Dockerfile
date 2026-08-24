FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["DistributedWorkflow.Analytics/DistributedWorkflow.Analytics.csproj", "DistributedWorkflow.Analytics/"]
RUN dotnet restore "DistributedWorkflow.Analytics/DistributedWorkflow.Analytics.csproj"

COPY . .
WORKDIR "/src/DistributedWorkflow.Analytics"
RUN dotnet publish "DistributedWorkflow.Analytics.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:9.0 AS final
WORKDIR /app

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "DistributedWorkflow.Analytics.dll"]