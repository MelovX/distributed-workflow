FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["Interview.Playground.Api/Interview.Playground.Api.csproj", "Interview.Playground.Api/"]
RUN dotnet restore "Interview.Playground.Api/Interview.Playground.Api.csproj"

COPY . .
WORKDIR "/src/Interview.Playground.Api"
RUN dotnet publish "Interview.Playground.Api.csproj" \
    -c Release \
    -o /app/publish \
    --no-restore \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

EXPOSE 8080

COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Interview.Playground.Api.dll"]