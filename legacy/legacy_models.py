import secrets
import uuid

from django.contrib.auth.base_user import BaseUserManager
from django.contrib.auth.hashers import make_password
from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils import timezone
from django.db.models import Q
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.translation import gettext_lazy as _


class UserManager(BaseUserManager):

    def _create_user(self, email, password=None, **extra_fields):
        if not email:
            raise AttributeError("User must set an email address")
        else:
            email = self.normalize_email(email)
        user = self.model(email=email, **extra_fields)
        user.password = make_password(password)
        user.save(using=self._db)
        return user

    def create_user(self, email, password=None, **extra_fields):
        extra_fields.setdefault("is_staff", False)
        extra_fields.setdefault("is_superuser", False)
        return self._create_user(email, password, **extra_fields)

    def create_staffuser(self, email, password=None, **extra_fields):
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("is_superuser", False)
        return self._create_user(email, password, **extra_fields)

    def create_superuser(self, email, password=None, **extra_fields):
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("is_superuser", True)
        return self._create_user(email, password, **extra_fields)


class UserProfile(AbstractUser):
    username = None
    first_name = None
    last_name = None
    is_retention = models.BooleanField(default=False)
    is_operator = models.BooleanField(default=None, null=True, unique=True)
    is_teamlead = models.BooleanField(default=False)
    name = models.CharField(_("Last name"), blank=True, max_length=150, db_index=True)
    email = models.EmailField(_("email address"), unique=True, db_index=True)

    objects = UserManager()

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = ('name',)

    def __str__(self):
        return '{}'.format(self.name)

    def save(self, *args, **kwargs):
        if not self.password.startswith("pbkdf2_sha256$"):
            self.password = make_password(self.password)

        return super(UserProfile, self).save(*args, **kwargs)

    class Meta:
        ordering = ('-is_staff', '-is_active', 'name',)
        verbose_name = 'Пользователь'
        verbose_name_plural = 'Пользователи'


class AbstractNamedModel(models.Model):
    name = models.CharField(max_length=127, db_index=True)

    class Meta:
        abstract = True
        ordering = ('name',)

    def __str__(self):
        return f'{self.name}'


class Database(AbstractNamedModel):
    owner = models.ForeignKey(UserProfile, on_delete=models.SET_NULL, null=True, related_name='database_owner',
                              verbose_name='Владелец', )
    allow_direct_push = models.BooleanField(default=True, verbose_name='Разрешить пуш без проверки')

    class Meta:
        verbose_name = 'База'
        verbose_name_plural = 'Базы'
        ordering = ('name',)


class Status(AbstractNamedModel):
    color = models.CharField(max_length=150, default='transparent', null=True, blank=True)
    tid = models.CharField(max_length=150, default='default', db_index=True, unique=True,
                           verbose_name='Text identifier')
    name_en = models.CharField(max_length=127, null=True, blank=True)
    is_valid = models.BooleanField(default=True, db_index=True, verbose_name='Валидный')

    def save(self, *args, **kwargs):
        self.tid = self.tid.lower().strip()
        return super().save(*args, **kwargs)

    class Meta:
        verbose_name = 'Статус'
        verbose_name_plural = 'Статусы'
        ordering = ('name',)


class Lead(models.Model):
    user = models.ForeignKey(UserProfile, on_delete=models.SET_NULL, related_name='user_leads', null=True,
                             verbose_name='Пользователь')
    database = models.ForeignKey(Database, on_delete=models.PROTECT, related_name="database_leads",
                                 verbose_name='База данных')

    status = models.ForeignKey(Status, on_delete=models.PROTECT, verbose_name='Статус лида', null=True)

    name = models.CharField(max_length=100, verbose_name='Имя', db_index=True)
    phone = models.CharField(max_length=127, verbose_name='Телефон', db_index=True)
    email = models.EmailField(max_length=100, null=True, blank=True, verbose_name='Почта')
    geo = models.CharField(max_length=15, null=True, blank=True, verbose_name='Гео')

    dialer = models.BooleanField(default=False, blank=True, null=True)

    temp_dep = models.PositiveSmallIntegerField(blank=True, null=True, verbose_name='Новый деп')
    temp_comment = models.TextField(blank=True, null=True, verbose_name='Новый коммент')

    created_at = models.DateTimeField(auto_now_add=True, verbose_name='Создан')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='Изменен')

    def __str__(self):
        return f'{self.name}'

    class Meta:
        ordering = ('-created_at',)
        verbose_name = 'Лид'
        verbose_name_plural = 'Лиды'


class Deposit(models.Model):
    DEP_TYPES = (
        (1, 'FTD'),
        (2, 'Reload'),
        (3, 'Deposit'),
    )
    creator = models.ForeignKey(UserProfile, on_delete=models.PROTECT, related_name='created_deps',
                                verbose_name='Создатель')
    lead = models.ForeignKey(Lead, on_delete=models.PROTECT, related_name='lead_deps', null=True, blank=True,
                             verbose_name='Лид')
    amount = models.DecimalField(max_digits=7, decimal_places=2, verbose_name="Сумма")
    created_at = models.DateTimeField(default=timezone.now, verbose_name='Деп создан')
    type = models.PositiveSmallIntegerField(default=3, choices=DEP_TYPES, verbose_name="Тип депозита")

    def __str__(self):
        return f'{self.lead} - {self.amount} - {self.creator}'

    class Meta:
        ordering = ('-created_at',)
        verbose_name = 'Депозит'
        verbose_name_plural = 'Депозиты'


@receiver(post_save, sender=Deposit)
def update_lead_updated_at(sender, instance, **kwargs):
    if instance.lead:
        instance.lead.save()


class Comment(models.Model):
    user = models.ForeignKey(UserProfile, on_delete=models.SET_NULL, related_name='user_comments', null=True)
    lead = models.ForeignKey(Lead, on_delete=models.CASCADE, related_name='lead_comments', null=True, default=None)
    comment = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    is_pinned = models.BooleanField(default=False, verbose_name='Закрепить?')

    def __str__(self):
        return self.comment[:30]

    class Meta:
        ordering = ('-is_pinned', '-created_at',)
        verbose_name = 'Коммент'
        verbose_name_plural = 'Комменты'


@receiver(post_save, sender=Comment)
def update_lead_updated_at(sender, instance, **kwargs):
    if instance.lead:
        instance.lead.save()


class Record(models.Model):
    author = models.ForeignKey(UserProfile, on_delete=models.SET_NULL, related_name='record_author',
                               verbose_name='Автор', null=True, default=None)
    comment = models.OneToOneField(Comment, on_delete=models.CASCADE, related_name='record', verbose_name='Коммент',
                                   null=True, default=None)
    lead = models.ForeignKey(Lead, on_delete=models.CASCADE, related_name='lead_records', verbose_name='Лид')

    record = models.FileField(upload_to='records/%Y/%m/%d', null=True, blank=True, verbose_name='Запись')
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ('-created_at',)
        verbose_name = 'Запись'
        verbose_name_plural = 'Записи'


class History(models.Model):
    statuses = (
        ('dep', 'Сделан депозит'),
        ('transfer', 'Трансфер лида'),
    )
    initiator = models.ForeignKey(UserProfile, on_delete=models.SET_NULL, related_name='initiator_history', null=True,
                                  blank=True)
    message = models.TextField()
    lead = models.ForeignKey(Lead, on_delete=models.SET_NULL, related_name='lead_history', null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ('-created_at',)
        verbose_name = 'Запись истории'
        verbose_name_plural = 'История'


class Transfer(models.Model):
    initiator = models.ForeignKey(UserProfile, on_delete=models.SET_NULL, related_name='initiator_transfer', null=True,
                                  verbose_name='Инициатор',
                                  blank=True)
    from_user = models.ForeignKey(UserProfile, on_delete=models.SET_NULL, related_name='transfer_from', null=True,
                                  verbose_name='От юзера',
                                  blank=True)
    to_user = models.ForeignKey(UserProfile, on_delete=models.SET_NULL, related_name='transfer_to', null=True,
                                verbose_name='К юзеру',
                                blank=True)
    lead = models.ForeignKey(Lead, on_delete=models.CASCADE, related_name='lead_transfer', null=True, blank=True,
                             verbose_name='Лид', )
    created_at = models.DateTimeField(auto_now_add=True, verbose_name='Создан', )

    class Meta:
        ordering = ('-created_at',)
        verbose_name = 'Трансфер'
        verbose_name_plural = 'Трансферы'


class Partner(models.Model):
    name = models.CharField(max_length=150, verbose_name='Название')
    access = models.CharField(max_length=255, verbose_name='Токен доступа')
    domain = models.URLField(max_length=255, verbose_name='Домен')
    api_endpoint = models.CharField(max_length=150, verbose_name='Точка входа')

    def __str__(self):
        return self.name

    class Meta:
        ordering = ('-name',)
        verbose_name = 'Партнер'
        verbose_name_plural = 'Партнеры'