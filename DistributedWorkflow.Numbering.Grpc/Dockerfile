FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["DistributedWorkflow.Numbering.Grpc/DistributedWorkflow.Numbering.Grpc.csproj", "DistributedWorkflow.Numbering.Grpc/"]
RUN dotnet restore "DistributedWorkflow.Numbering.Grpc/DistributedWorkflow.Numbering.Grpc.csproj"

COPY . .
WORKDIR "/src/DistributedWorkflow.Numbering.Grpc"
RUN dotnet publish "DistributedWorkflow.Numbering.Grpc.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

EXPOSE 8080

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "DistributedWorkflow.Numbering.Grpc.dll"]