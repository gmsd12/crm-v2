from django.contrib import admin
from django.urls import path, include
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView
from apps.leads.api.views import protected_media

urlpatterns = [
    path("admin/", admin.site.urls),
    path("media/<path:file_path>", protected_media, name="protected-media"),
    path("api/", include("apps.core.api.urls")),
    path("api/v1/", include("apps.iam.api.urls")),
    path("api/v1/", include("apps.partners.api.urls")),
    path("api/v1/", include("apps.leads.api.urls")),
    # OpenAPI schema + docs
    path("api/schema/", SpectacularAPIView.as_view(), name="schema"),
    path("api/docs/", SpectacularSwaggerView.as_view(url_name="schema"), name="swagger-ui"),
]
