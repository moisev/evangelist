package com.github.moisvla.evangelist

class CustomException(private val message: String = "",
                      private val cause: Throwable = None.orNull)
                      extends Exception(message, cause)

case class IncompleteArgumentList(message: String = "The argument list is missing or incomplete",
                                  nestedException: Throwable = None.orNull)
                                  extends CustomException(message,nestedException)

case class IOEmptyFile(message: String = "The file is empty or does not exist. Cannot perform action on it",
                                  nestedException: Throwable = None.orNull)
  extends CustomException(message,nestedException)