FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["Interview.Playground.Numbering.Grpc/Interview.Playground.Numbering.Grpc.csproj", "Interview.Playground.Numbering.Grpc/"]
RUN dotnet restore "Interview.Playground.Numbering.Grpc/Interview.Playground.Numbering.Grpc.csproj"

COPY . .
WORKDIR "/src/Interview.Playground.Numbering.Grpc"
RUN dotnet publish "Interview.Playground.Numbering.Grpc.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

EXPOSE 8080

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Interview.Playground.Numbering.Grpc.dll"]