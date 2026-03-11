from rest_framework.decorators import api_view
from rest_framework.response import Response


@api_view(["GET"])
def health(request):
    request.logger.info("проверка здоровья")

    return Response({"status": "ок"})
